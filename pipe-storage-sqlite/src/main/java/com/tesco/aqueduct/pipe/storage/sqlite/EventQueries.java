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
            "DELETE FROM EVENT WHERE created_utc <= ? AND msg_offset NOT IN (SELECT max(msg_offset) FROM EVENT GROUP BY msg_key);";

    static String getReadEvent(final int typesCount, final long maxBatchSize) {
        final StringBuilder queryBuilder = new StringBuilder(
            " SELECT type, msg_key, content_type, msg_offset, created_utc, data " +
            " FROM " +
            "   (" +
            "      SELECT " +
            "        type, msg_key, content_type, msg_offset, created_utc, data, " +
            "        SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size " +
            "      FROM event  " +
            "      WHERE msg_offset >= ? " +
            "   ) unused "
        );

        if (typesCount == 0) {
            queryBuilder.append(" WHERE running_size <= ");
            queryBuilder.append(maxBatchSize);
        } else {
            queryBuilder
                .append(" WHERE type IN (")
                .append(generateQuestionMarks(typesCount))
                .append(")");

            queryBuilder.append(" AND running_size <= ");
            queryBuilder.append(maxBatchSize);
        }

        queryBuilder.append(" LIMIT ?;");

        return queryBuilder.toString();
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
