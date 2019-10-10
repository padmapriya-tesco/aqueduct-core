package com.tesco.aqueduct.registry;

import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class TillStorageLogger {

    private final Logger log;
    private final Map<String, String> fields;

    public TillStorageLogger(Logger logger) {
        this.log = logger;
        this.fields = new HashMap<>();
    }

    public void error(final String where, final String what, final Exception why) {
        log(where, what, why, log::error);
    }


    private void log(final String where, final String what, final Consumer<String> loggerFunc) {
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what);
        } finally {
            MDC.clear();
        }
    }

    private void log(final String where, final String what, final Object why, final BiConsumer<String, Object> loggerFunc) {
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what, why);
        } finally {
            MDC.clear();
        }
    }

    public void info(final String where, final String what) {
        if (log.isInfoEnabled()) {
            log(where, what, log::info);
        }
    }
}
