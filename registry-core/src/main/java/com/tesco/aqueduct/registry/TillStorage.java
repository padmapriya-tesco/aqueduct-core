package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.BootstrapType;

public interface TillStorage {
    void updateTill(String hostId, BootstrapType bootstrapType);
}
