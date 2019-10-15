package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.Till;

import java.sql.SQLException;

public interface TillStorage {
    void updateTill(Till till) throws SQLException;
}
