package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PostgresqlStorage implements MessageReader {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PostgresqlStorage.class));

    private final int limit;
    private final DataSource dataSource;
    private final long maxBatchSize;
    private final long retryAfter;

    public PostgresqlStorage(final DataSource dataSource, final int limit, final long retryAfter, final long maxBatchSize) {
        this.retryAfter = retryAfter;
        this.limit = limit;
        this.dataSource = dataSource;
        this.maxBatchSize = maxBatchSize + (((long)Message.MAX_OVERHEAD_SIZE) * limit);
    }

    @Override
    public MessageResults read(final List<String> types, final long startOffset) {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement messagesQuery = getMessagesStatement(connection, types, startOffset)) {

            final long maxOffset = getLatestOffsetMatchingWithConnection(connection, types);
            final long retry = startOffset >= maxOffset ? retryAfter : 0;

            LOG.withTypes(types).debug("postgresql storage", "reading with types");

            final List<Message> messages = runMessagesQuery(messagesQuery);

            return new MessageResults(messages, retry);
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "read", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public long getLatestOffsetMatching(final List<String> types) {
        try (Connection connection = dataSource.getConnection()) {
            return getLatestOffsetMatchingWithConnection(connection, types);
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offeset matching", exception);
            throw new RuntimeException(exception);
        }
    }

    private long getLatestOffsetMatchingWithConnection(final Connection connection, final List<String> types)
            throws SQLException {

        try (PreparedStatement statement = getLatestOffsetStatement(connection, types);
            ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();

            return resultSet.getLong("last_offset");
        }
    }

    private List<Message> runMessagesQuery(final PreparedStatement query) throws SQLException {
        final List<Message> messages = new ArrayList<>();

        try (ResultSet rs = query.executeQuery()) {
            while (rs.next()) {
                final String type = rs.getString("type");
                final String key = rs.getString("msg_key");
                final String contentType = rs.getString("content_type");
                final Long offset = rs.getLong("msg_offset");
                final ZonedDateTime created = ZonedDateTime.of(rs.getTimestamp("created_utc").toLocalDateTime(), ZoneId.of("UTC"));
                final String data = rs.getString("data");

                messages.add(new Message(type, key, contentType, offset, created, data));
            }
        }
        return messages;
    }

    private PreparedStatement getLatestOffsetStatement(final Connection connection, final List<String> types) {

        try {
            PreparedStatement query;

            if (types == null || types.isEmpty()) {
                query = connection.prepareStatement(getSelectLatestOffsetWithoutTypeQuery());
            } else {
                final String strTypes = String.join(",", types);
                query = connection.prepareStatement(getSelectLatestOffsetWithTypeQuery());
                query.setString(1, strTypes);
            }

            return query;

        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offset statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private PreparedStatement getMessagesStatement(final Connection connection, final List<String> types, final long startOffset) {
        try {
            PreparedStatement query;

            if (types == null || types.isEmpty()) {
                query = connection.prepareStatement(getSelectEventsWithoutTypeFilteringQuery(maxBatchSize));
                query.setLong(1, startOffset);
                query.setLong(2, limit);

            } else {
                final String strTypes = String.join(",", types);

                query = connection.prepareStatement(getSelectEventsWithTypeFilteringQuery(maxBatchSize));
                query.setLong(1, startOffset);
                query.setString(2, strTypes);
                query.setLong(3, limit);
            }

            return query;

        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get message statement", exception);
            throw new RuntimeException(exception);
        }
    }

    public void compactUpTo(final ZonedDateTime thresholdDate) {
        try(Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(getCompactionQuery())) {
                Timestamp threshold = Timestamp.valueOf(thresholdDate.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
                statement.setTimestamp(1, threshold);
                statement.setTimestamp(2, threshold);
                final int rowsAffected = statement.executeUpdate();
                LOG.info("compaction", "compacted " + rowsAffected + " rows");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO: use parameter for batch size
    private static String getSelectEventsWithTypeFilteringQuery(final long maxBatchSize) {
        return
            " SELECT type, msg_key, content_type, msg_offset, created_utc, data " +
            " FROM " +
            " ( " +
            "   SELECT " +
            "     type, msg_key, content_type, msg_offset, created_utc, data, " +
            "     SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size " +
            "   FROM events " +
            "   WHERE " +
            "     msg_offset >= ? " +
            "     AND type = ANY (string_to_array(?, ',')" +
            " ) " +
            " ORDER BY msg_offset " +
            " LIMIT ?" +
            " ) unused " +
            " WHERE running_size <= " + maxBatchSize;
    }

    //TODO: use parameter for batch size
    private static String getSelectEventsWithoutTypeFilteringQuery(final long maxBatchSize) {
        return
            " SELECT type, msg_key, content_type, msg_offset, created_utc, data " +
            " FROM " +
            " (" +
            "   SELECT " +
            "     type, msg_key, content_type, msg_offset, created_utc, data, " +
            "     SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size " +
            "   FROM events " +
            "   WHERE msg_offset >= ? " +
            "   ORDER BY msg_offset " +
            "   LIMIT ? " +
            " ) unused " +
            " WHERE running_size <= " + maxBatchSize;
    }

    private static String getSelectLatestOffsetWithTypeQuery() {
        return
            " SELECT coalesce(max(msg_offset),0) as last_offset " +
            " FROM events " +
            " WHERE type  = ANY (string_to_array(?, ','));";
    }

    private static String getSelectLatestOffsetWithoutTypeQuery() {
        return " SELECT coalesce(max(msg_offset),0) as last_offset FROM events ;";
    }

    private static String getCompactionQuery() {
        return
            " DELETE FROM events " +
            " WHERE msg_offset in " +
            " (" +
            "   SELECT msg_offset " +
            "   FROM " +
            "   events e, " +
            "   ( " +
            "     SELECT msg_key, max(msg_offset) max_offset " +
            "     FROM events " +
            "     WHERE " +
            "       created_utc <= ? " +
            "     GROUP BY msg_key " +
            "   ) x " +
            "   WHERE " +
            "     created_utc <= ? " +
            "     AND e.msg_key = x.msg_key AND e.msg_offset <> x.max_offset " +
            " );";
    }
}
