package com.tesco.aqueduct.pipe.storage.sqlite;

import java.util.stream.Collectors;
import java.util.stream.Stream;

final class EventQueries {
    static final String CREATE_EVENT_TABLE =
        "CREATE TABLE IF NOT EXISTS EVENT( " +
        " msg_offset bigint PRIMARY KEY NOT NULL," +
        " msg_key varchar NOT NULL," +
        " content_type varchar NULL," +
        " type varchar NOT NULL," +
        " created_utc timestamp NOT NULL," +
        " data text NULL," +
        " event_size int NOT NULL" +
        ");";

    static final String INSERT_EVENT =
            "INSERT INTO EVENT (msg_offset, msg_key, content_type, type, created_utc, data, event_size) VALUES (?,?,?,?,?,?,?);";

    static final String COMPACT =
            "DELETE FROM EVENT WHERE created_utc <= ? AND msg_offset NOT IN (SELECT max(msg_offset) FROM EVENT WHERE created_utc <= ? GROUP BY msg_key);";

    static String getReadEvent(final int typesCount, final long maxBatchSize) {
        final StringBuilder queryBuilder = new StringBuilder()
            .append(" SELECT type, msg_key, content_type, msg_offset, created_utc, data, event_size ")
            .append(" FROM ")
            .append("    ( ")
            .append("       SELECT ")
            .append("         \"type\", msg_key, content_type, msg_offset, created_utc, \"data\", event_size, ")
            .append("         SUM(event.event_size) OVER (ORDER BY event.msg_offset ASC) AS running_size ")
            .append("       FROM event ")
            .append("       WHERE msg_offset >= ? ");

        appendFilterByTypes(queryBuilder, typesCount);

        queryBuilder
            .append("       ORDER BY msg_offset ASC ")
            .append("       LIMIT ?")
            .append("    ) unused ")
            .append(" WHERE running_size <  ").append(maxBatchSize)
            .append(" ORDER BY msg_offset ASC;")
        ;

        return queryBuilder.toString();
    }

    static final String DELETE_ALL_EVENTS = "DELETE FROM EVENT;";
    static final String VACUUM_DB = "VACUUM;";
    static final String CHECKPOINT_DB = "PRAGMA wal_checkpoint(TRUNCATE);";

    static void appendFilterByTypes(final StringBuilder queryBuilder, int typesCount) {
        if (typesCount != 0) {
            queryBuilder
                .append(" AND type IN (")
                .append(generateQuestionMarks(typesCount))
                .append(")");
        }
    }

    static String getLastOffsetQuery(final int typesCount) {
        final StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT coalesce(max(msg_offset),0) as last_offset FROM EVENT");

        if (typesCount != 0) {
            queryBuilder
                .append(" WHERE type IN (")
                .append(generateQuestionMarks(typesCount))
                .append(")");
        }

        queryBuilder.append(";");

        return queryBuilder.toString();
    }

    private static String generateQuestionMarks(final int types) {
        return Stream.generate(() -> "?").limit(types).collect(Collectors.joining(","));
    }
}
