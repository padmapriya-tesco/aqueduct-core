package com.tesco.aqueduct.pipe.http;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.contrib.json.classic.JsonLayout;

import java.util.Map;

public class JsonLayoutWithVersion extends JsonLayout {
    private final String version = Version.getImplementationVersion();
    @Override
    protected void addCustomDataToJsonMap(final Map<String, Object> map, final ILoggingEvent event) {
        map.put("v", version);
    }
}
