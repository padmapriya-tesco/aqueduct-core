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
                    ");";

    public PostgreSQLTillStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void updateTill(String hostId, BootstrapType bootstrapType) {
         try (Connection connection = getConnection()) {
             insert(connection, hostId, bootstrapType);
             System.out.println("foo");
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

    private void insert(final Connection connection, final String hostId, final BootstrapType bootstrapType) throws SQLException {
        long start = System.currentTimeMillis();

        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now()
            .atOffset(ZoneOffset.UTC)
            .toLocalDateTime());

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
