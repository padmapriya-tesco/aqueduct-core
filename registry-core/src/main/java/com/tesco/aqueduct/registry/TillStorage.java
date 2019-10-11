package com.tesco.aqueduct.registry;

import com.tesco.aqueduct.registry.model.BootstrapType;

import java.time.LocalDateTime;

public interface TillStorage {
    void updateTill(String hostId, BootstrapType bootstrapType, LocalDateTime requestedDate);
}
