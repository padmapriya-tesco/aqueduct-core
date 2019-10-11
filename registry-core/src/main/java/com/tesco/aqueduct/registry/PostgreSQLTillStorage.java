package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.BootstrapType;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;


public class PostgreSQLTillStorage implements TillStorage {

    private final DataSource dataSource;

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLTillStorage.class));
    private static final String QUERY_INSERT_TILL =
        "INSERT INTO tills (host_id, bootstrap_requested, bootstrap_type)" +
            "VALUES (" +
            "?, " +
            "?, " +
            "? " +
            ")" +
            "ON CONFLICT (host_id) DO UPDATE SET " +
            "host_id = EXCLUDED.host_id, " +
            "bootstrap_requested = EXCLUDED.bootstrap_requested, " +
            "bootstrap_type = EXCLUDED.bootstrap_type;";

    public PostgreSQLTillStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void updateTill(final String hostId, final BootstrapType bootstrapType, final LocalDateTime requestedDate) {
         try (Connection connection = getConnection()) {
             insert(connection, hostId, bootstrapType, requestedDate);
         } catch (SQLException exception) {
             LOG.error("Postgresql till storage", "hostId", exception);
         }
    }

    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        try {
            return dataSource.getConnection();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getConnection:time", Long.toString(end - start));
        }
    }

    private void insert(
        final Connection connection,
        final String hostId,
        final BootstrapType bootstrapType,
        final LocalDateTime requestedDate
    ) throws SQLException {
        long start = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.valueOf(requestedDate.atOffset(ZoneOffset.UTC).toLocalDateTime());

        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_TILL)) {
            statement.setString(1, hostId);
            statement.setTimestamp(2, timestamp);
            statement.setString(3, bootstrapType.toString());
            statement.execute();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("hostId insert:time", Long.toString(end - start));
        }
    }

}
