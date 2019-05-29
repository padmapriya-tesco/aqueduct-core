package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.JsonHelper;
import com.tesco.aqueduct.pipe.api.Message;
import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.api.MessageResults;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PostgresqlStorage implements MessageReader {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PostgresqlStorage.class));

    private final int limit;
    private final DataSource dataSource;
    private final long maxBatchSize;
    private final long retryAfter;

    public PostgresqlStorage(DataSource dataSource, int limit, long retryAfter, long maxBatchSize) {
        this.retryAfter = retryAfter;
        this.limit = limit;
        this.dataSource = dataSource;
        this.maxBatchSize = maxBatchSize + (Message.MAX_OVERHEAD_SIZE * limit);
    }

    @Override
    public MessageResults read(Map<String, List<String>> tags, long startOffset) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement messagesQuery = getMessagesStatement(connection, tags, startOffset)) {

            long maxOffset = getLatestOffsetMatchingWithConnection(connection, tags);
            long retry = startOffset >= maxOffset ? retryAfter : 0;

            LOG.withTags(tags).debug("postgresql storage", "reading with tags");

            List<Message> messages = runMessagesQuery(messagesQuery);

            return new MessageResults(messages, retry);
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "read", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public long getLatestOffsetMatching(Map<String, List<String>> tags) {
        try (Connection connection = dataSource.getConnection()) {
            return getLatestOffsetMatchingWithConnection(connection, tags);
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offeset matching", exception);
            throw new RuntimeException(exception);
        }
    }

    private long getLatestOffsetMatchingWithConnection(Connection connection, Map<String, List<String>> tags)
            throws SQLException {

        try (PreparedStatement statement = getLatestOffsetStatement(connection, tags);
            ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();

            return resultSet.getLong("last_offset");
        }
    }

    private List<Message> runMessagesQuery(PreparedStatement query) throws SQLException {
        List<Message> messages = new ArrayList<>();

        try (ResultSet rs = query.executeQuery()) {
            while (rs.next()) {
                String type = rs.getString("type");
                String key = rs.getString("msg_key");
                String contentType = rs.getString("content_type");
                Long offset = rs.getLong("msg_offset");

                ZonedDateTime created = ZonedDateTime.of(rs.getTimestamp("created_utc").toLocalDateTime(), ZoneId.of("UTC"));

                Map<String, List<String>> msgTags = parseTags(rs.getString("tags"));
                String data = rs.getString("data");

                messages.add(new Message(type, key, contentType, offset, created, msgTags, data));
            }
        }
        return messages;
    }

    private PreparedStatement getLatestOffsetStatement(Connection connection, Map<String, List<String>> tags) {

        try {
            PreparedStatement query;

            if (tags == null || tags.isEmpty()) {
                query = connection.prepareStatement(getSelectLatestOffsetWithoutTypeQuery(false));

            } else if (tags.containsKey("type")) {
                Map<String, List<String>> tagsWithoutType = new HashMap<>(tags);

                String types = String.join(",", tags.get("type"));
                tagsWithoutType.remove("type");

                if (tagsWithoutType.isEmpty()) {
                    query = connection.prepareStatement(getSelectLatestOffsetWithTypeQuery(false));
                    query.setString(1, types);
                } else {
                    query = connection.prepareStatement(getSelectLatestOffsetWithTypeQuery(true));
                    query.setString(1, types);
                    query.setString(2, JsonHelper.toJson(tagsWithoutType));
                }
            } else {
                query = connection.prepareStatement(getSelectLatestOffsetWithoutTypeQuery(true));
                query.setString(1, JsonHelper.toJson(tags));
            }

            return query;

        } catch (SQLException | IOException exception) {
            LOG.error("postgresql storage", "get latest offset statement", exception);

            throw new RuntimeException(exception);
        }
    }

    private PreparedStatement getMessagesStatement(Connection connection, Map<String, List<String>> tags, long startOffset) {
        try {
            PreparedStatement query;

            if (tags == null || tags.isEmpty()) {
                query = connection.prepareStatement(getSelectEventsWithoutTypeFilteringQuery(false, maxBatchSize));
                query.setLong(1, startOffset);
                query.setLong(2, limit);

            } else if (tags.containsKey("type")) {
                Map<String, List<String>> tagsWithoutType = new HashMap<>(tags);
                String types = String.join(",", tags.get("type"));
                tagsWithoutType.remove("type");

                if (tagsWithoutType.isEmpty()) {
                    query = connection.prepareStatement(getSelectEventsWithTypeFilteringQuery(false, maxBatchSize));
                    query.setLong(1, startOffset);
                    query.setString(2, types);
                    query.setLong(3, limit);
                } else {
                    query = connection.prepareStatement(getSelectEventsWithTypeFilteringQuery(true, maxBatchSize));
                    query.setLong(1, startOffset);
                    query.setString(2, types);
                    query.setString(3, JsonHelper.toJson(tagsWithoutType));
                    query.setLong(4, limit);
                }
            } else {
                query = connection.prepareStatement(getSelectEventsWithoutTypeFilteringQuery(true, maxBatchSize));
                query.setLong(1, startOffset);
                query.setString(2, JsonHelper.toJson(tags));
                query.setLong(3, limit);
            }

            return query;

        } catch (SQLException | IOException exception) {
            LOG.error("postgresql storage", "get message statement", exception);
            throw new RuntimeException(exception);
        }
    }

    private static Map<String, List<String>> parseTags(String tags) {
        if (tags == null) {
            return Collections.emptyMap();
        }

        try {
            return JsonHelper.tagsFromJson(tags);
        } catch (IOException exception) {
            LOG.error("postgresql storage", "parse tags", exception);
            throw new RuntimeException(exception);
        }
    }

    private static String getSelectEventsWithTypeFilteringQuery(boolean tags, long maxBatchSize) {
        return "SELECT type, msg_key, content_type, msg_offset, created_utc, tags, data FROM " +
                "(SELECT type, msg_key, content_type, msg_offset, created_utc, tags, data, " +
                "SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size FROM events " +
                " WHERE msg_offset >= ? " +
                " AND type = ANY (string_to_array(?, ',')) " +
                (tags ? " AND tags @> ?::JSONB " : "") +
                " ORDER BY msg_offset " +
                " LIMIT ?) unused WHERE running_size <= " + maxBatchSize;
    }

    private static String getSelectEventsWithoutTypeFilteringQuery(boolean tags, long maxBatchSize) {
        return "SELECT type, msg_key, content_type, msg_offset, created_utc, tags, data FROM " +
                "(SELECT type, msg_key, content_type, msg_offset, created_utc, tags, data, " +
                "SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size FROM events " +
                " WHERE msg_offset >= ? " +
                (tags ? " AND tags @> ?::JSONB " : "") +
                " ORDER BY msg_offset " +
                " LIMIT ?) unused WHERE running_size <= " + maxBatchSize;
    }

    private static String getSelectLatestOffsetWithTypeQuery(boolean tags) {
        return "SELECT coalesce(max(msg_offset),0) as last_offset FROM events " +
                " WHERE " +
                " type  = ANY (string_to_array(?, ',')) " +
                (tags ? " AND tags @> ?::JSONB;" : ";");
    }

    private static String getSelectLatestOffsetWithoutTypeQuery(boolean tags) {
        return " SELECT coalesce(max(msg_offset),0) as last_offset FROM events " +
                (tags ? " WHERE tags @> ?::JSONB;" : ";");
    }
}
