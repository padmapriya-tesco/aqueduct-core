package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET;

public class SQLiteStorage implements DistributedStorage {

    private final DataSource dataSource;
    private final int limit;
    private final int retryAfterSeconds;
    private final long maxBatchSize;

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(SQLiteStorage.class));

    public SQLiteStorage(final DataSource dataSource, final int limit, final int retryAfterSeconds, final long maxBatchSize) {
        this.dataSource = dataSource;
        this.limit = limit;
        this.retryAfterSeconds = retryAfterSeconds;
        this.maxBatchSize = maxBatchSize + (((long)Message.MAX_OVERHEAD_SIZE) * limit);

        createEventTableIfNotExists();
        createOffsetTableIfNotExists();
        createPipeStateTableIfNotExists();
    }

    private void createEventTableIfNotExists() {
        execute(
            SQLiteQueries.CREATE_EVENT_TABLE,
            (connection, statement) -> statement.execute()
        );
    }

    private void createOffsetTableIfNotExists() {
        execute(
            SQLiteQueries.OFFSET_TABLE,
            (connection, statement) -> statement.execute()
        );
    }

    private void createPipeStateTableIfNotExists() {
        execute(
            SQLiteQueries.PIPE_STATE_TABLE,
            (Connection, statement) -> statement.execute()
        );
    }

    @Override
    public MessageResults read(final List<String> types, final long offset, final List<String> locationUuids) {
        final List<Message> retrievedMessages = new ArrayList<>();
        final int typesCount = types == null ? 0 : types.size();

        /*
         * Assumption is that reading offset and state before messages will be consistent, could be wrong
         * We think this is better than before, but needs more investigation in the future
         */
        OptionalLong globalLatestOffset = getOffset(GLOBAL_LATEST_OFFSET);
        PipeState pipeState = getPipeState();

        execute(
            SQLiteQueries.getReadEvent(typesCount, maxBatchSize),
            (connection, statement) -> {
                int parameterIndex = 1;
                statement.setLong(parameterIndex++, offset);

                for (int i = 0; i < typesCount; i++, parameterIndex++) {
                    statement.setString(parameterIndex, types.get(i));
                }

                statement.setLong(parameterIndex, limit);

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        retrievedMessages.add(mapRetrievedMessageFromResultSet(resultSet));
                    }
                }
            }
        );

        return new MessageResults(retrievedMessages, calculateRetryAfter(retrievedMessages.size()), globalLatestOffset, pipeState);
    }

    @Override
    public PipeState getPipeState() {
        return executeGet(
            SQLiteQueries.GET_PIPE_STATE,
            (connection, statement) -> {
                try (ResultSet resultSet = statement.executeQuery()) {
                    return resultSet.next()
                        ? PipeState.valueOf(resultSet.getString("value"))
                        : PipeState.UNKNOWN;
                }
            }
        );
    }

    public int calculateRetryAfter(final int messageCount) {
        return messageCount > 0 ? 0 : retryAfterSeconds;
    }

    private Message mapRetrievedMessageFromResultSet(final ResultSet resultSet) throws SQLException {
        Message retrievedMessage;
        final ZonedDateTime time = ZonedDateTime.of(
            resultSet.getTimestamp("created_utc").toLocalDateTime(),
            ZoneId.of("UTC")
        );

        retrievedMessage = new Message(
            resultSet.getString("type"),
            resultSet.getString("msg_key"),
            resultSet.getString("content_type"),
            resultSet.getLong("msg_offset"),
            time,
            resultSet.getString("data"),
            resultSet.getLong("event_size")
        );

        return retrievedMessage;
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        return executeGet(
            SQLiteQueries.getOffset(offsetName),
            (connection, statement) -> {
                ResultSet resultSet = statement.executeQuery();

                return resultSet.next() ?
                    OptionalLong.of(resultSet.getLong("value")) : OptionalLong.empty();
            }
        );
    }

    @Override
    public void write(final Iterable<Message> messages) {
        execute(SQLiteQueries.INSERT_EVENT,
            (connection, statement) -> {
                connection.setAutoCommit(false);

                for (final Message message : messages) {
                    setStatementParametersForInsertMessageQuery(statement, message);
                    statement.addBatch();
                }

                statement.executeBatch();
                connection.commit();
            });
    }

    @Override
    public void write(final Message message) {
        execute(
            SQLiteQueries.INSERT_EVENT,
            (connection, statement) -> {
                setStatementParametersForInsertMessageQuery(statement, message);
                statement.execute();
            }
        );
    }

    @Override
    public void write(OffsetEntity offset) {
        execute(
            SQLiteQueries.UPSERT_OFFSET,
            (connection, statement) -> {
                statement.setString(1, offset.getName().toString());
                statement.setLong(2, offset.getValue().getAsLong());
                statement.setLong(3, offset.getValue().getAsLong());
                statement.execute();
            }
        );
    }

    @Override
    public void write(PipeState pipeState) {
        execute(
            SQLiteQueries.UPSERT_PIPE_STATE,
            ((connection, statement) -> {
                statement.setString(1, "pipe_state");
                statement.setString(2, pipeState.toString());
                statement.setString(3, pipeState.toString());
                statement.execute();
            })
        );
    }

    @Override
    public void runVisibilityCheck() {
        runIntegrityCheck();
    }

    private void runIntegrityCheck() {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(SQLiteQueries.INTEGRITY_CHECK)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                String result = resultSet.getString(1);
                if (!result.equals("ok")) {
                    LOG.error("integrity check", "integrity check failed", result);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void execute(String query, SqlConsumer consumer) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            consumer.accept(connection, statement);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    private <T> T executeGet(String query, SqlFunction<T> function) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            return function.apply(connection, statement);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    private interface SqlConsumer {
        void accept(Connection connection, PreparedStatement statement) throws SQLException;
    }

    private interface SqlFunction<T> {
        T apply(Connection connection, PreparedStatement statement) throws SQLException;
    }

    @Override
    public void deleteAll() {
        try (Connection connection = dataSource.getConnection()){
            deleteEvents(connection);
            deleteOffsets(connection);
            deletePipeState(connection);
            vacuumDatabase(connection);
            checkpointWalFile(connection);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    private void deleteOffsets(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_OFFSETS)) {
            statement.execute();
            LOG.info("deleteOffsets", String.format("Delete offsets result: %d", statement.getUpdateCount()));
        }
    }

    private void deleteEvents(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_EVENTS)) {
            statement.execute();
            LOG.info("deleteAllEvents", String.format("Delete events result: %d", statement.getUpdateCount()));
        }
    }

    private void deletePipeState(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_PIPE_STATE)) {
            statement.execute();
            LOG.info("deletePipeState", String.format("Delete pipe state result: %d", statement.getUpdateCount()));
        }
    }

    private void vacuumDatabase(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.VACUUM_DB)) {
            statement.execute();
            LOG.info("vacuumDatabase", String.format("Vacuum result: %d", statement.getUpdateCount()));
        }
    }

    private void checkpointWalFile(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.CHECKPOINT_DB)) {
            statement.execute();
            LOG.info("checkPointDatabase", "checkpointed database");
        }
    }

    public void compactUpTo(final ZonedDateTime zonedDateTime) {
        // We may want a interface - Compactable - for this method signature
        try (Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(SQLiteQueries.COMPACT)) {
            Timestamp threshold = Timestamp.valueOf(zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
            statement.setTimestamp(1, threshold);
            statement.setTimestamp(2, threshold);
            final int rowsAffected = statement.executeUpdate();
            LOG.info("compaction", "compacted " + rowsAffected + " rows");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setStatementParametersForInsertMessageQuery(
            final PreparedStatement statement, final Message message) throws SQLException {
        try {
            statement.setLong(1, message.getOffset());
            statement.setString(2, message.getKey());
            statement.setString(3, message.getContentType());
            statement.setString(4, message.getType());
            statement.setTimestamp(5, Timestamp.valueOf(message.getCreated().withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime()));
            statement.setString(6, message.getData());
            statement.setInt(7, JsonHelper.toJson(message).length());
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}