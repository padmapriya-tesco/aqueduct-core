package com.tesco.aqueduct.registry.postgres;

import com.tesco.aqueduct.registry.model.BootstrapType;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import com.tesco.aqueduct.registry.model.Till;
import com.tesco.aqueduct.registry.model.TillStorage;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class PostgreSQLTillStorage implements TillStorage {

    private final DataSource dataSource;
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLTillStorage.class));
    private static final String QUERY_INSERT_OR_UPDATE_TILL =
        "INSERT INTO tills (host_id, bootstrap_requested, bootstrap_type)" +
            "VALUES (" +
            "?, " +
            "?, " +
            "? " +
            ")" +
        "ON CONFLICT (host_id) DO UPDATE SET " +
            "host_id = EXCLUDED.host_id, " +
            "bootstrap_requested = EXCLUDED.bootstrap_requested, " +
            "bootstrap_type = EXCLUDED.bootstrap_type, " +
            "bootstrap_received = null;";
    private static final String QUERY_READ_TILL =
        "SELECT bootstrap_type " +
        "FROM tills " +
        "WHERE host_id = ? AND bootstrap_received IS null;";
    private static final String QUERY_UPDATE_TILL_RECEIVED =
        "UPDATE tills " +
        "SET bootstrap_received = ? " +
        "WHERE host_id = ?;";

    public PostgreSQLTillStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Till till) throws SQLException {
         try (Connection connection = getConnection()) {
             insertOrUpdate(connection, till);
         } catch (SQLException exception) {
             LOG.error("save", "insert a till", exception);
             throw exception;
         }
    }

    @Override
    public BootstrapType requiresBootstrap(String hostId) throws SQLException {
        try (Connection connection = getConnection()) {
            BootstrapType bootstrapType = readBootstrapType(hostId, connection);
            if (bootstrapType != BootstrapType.NONE) {
                updateReceivedBootstrap(connection, hostId);
            }
            return bootstrapType;
        } catch (SQLException exception) {
            LOG.error("read", "read a till", exception);
            throw exception;
        }
    }

    private BootstrapType readBootstrapType(String hostId, Connection connection) throws SQLException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(QUERY_READ_TILL)) {
            statement.setString(1, hostId);
            return getBootstrapType(statement.executeQuery());
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("read:time", Long.toString(end - start));
        }
    }

    private BootstrapType getBootstrapType(ResultSet executeQuery) throws SQLException {
         if(executeQuery.next()) {
             String bootstrapType = executeQuery.getString("bootstrap_type");
             return BootstrapType.valueOf(bootstrapType);
         }
         return BootstrapType.NONE;
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

    private void updateReceivedBootstrap(Connection connection, String hostId) throws SQLException {
        long start = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.valueOf(ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime());
        try (PreparedStatement statement = connection.prepareStatement(QUERY_UPDATE_TILL_RECEIVED)) {
            statement.setTimestamp(1, timestamp);
            statement.setString(2, hostId);
            statement.execute();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("updateReceived:time", Long.toString(end - start));
        }
    }

    private void insertOrUpdate(
        final Connection connection,
        final Till till
    ) throws SQLException {
        long start = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.valueOf(till.getBootstrap().getRequestedDate().atOffset(ZoneOffset.UTC).toLocalDateTime());

        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_OR_UPDATE_TILL)) {
            statement.setString(1, till.getHostId());
            statement.setTimestamp(2, timestamp);
            statement.setString(3, till.getBootstrap().getType().toString());
            statement.execute();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("insert:time", Long.toString(end - start));
        }
    }
}
