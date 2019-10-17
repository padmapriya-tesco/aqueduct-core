package com.tesco.aqueduct.registry.model;

import java.sql.SQLException;

public interface TillStorage {
    void save(Till till) throws SQLException;
    BootstrapType requiresBootstrap(String hostId) throws SQLException;
}
