package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.BootstrapType;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;


public class PostgreSQLTillStorage implements TillStorage {

    private final DataSource dataSource;

    private static final TillStorageLogger LOG = new TillStorageLogger(LoggerFactory.getLogger(PostgreSQLTillStorage.class));
    private static final String QUERY_INSERT_GROUP =
            "INSERT INTO tills (hostId, bootstrapRequested, bootstrapType)" +
                    "VALUES (" +
                    "?, " +
                    "?, " +
                    "? " +
                    ")" +
                    "ON CONFLICT DO UPDATE ;";

    public PostgreSQLTillStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void updateTill(String hostId, BootstrapType bootstrapType) {
         try {
             Connection connection = getConnection();
             insert(connection, hostId, bootstrapType);
             } catch (SQLException | IOException exception) {
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

    private void insert(final Connection connection, final String hostId, final BootstrapType bootstrapType) throws IOException, SQLException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_GROUP)) {
            statement.setString(1, hostId);
            statement.setString(2, ZonedDateTime.now().toString());
            statement.setString(3, bootstrapType.toString());
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("hostId insert:time", Long.toString(end - start));
        }
    }

}
