package com.tesco.aqueduct.registry;

import java.net.URL;
import java.util.List;

public interface RegistryHitList {
    void update(List<URL> services);

    List<URL> getFollowing();
}
