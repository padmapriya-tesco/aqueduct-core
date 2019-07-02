package com.tesco.aqueduct.pipe.logger;

import com.tesco.aqueduct.pipe.api.Message;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class PipeLogger {

    private final Logger log;

    private final Map<String, String> fields;

    public PipeLogger(Logger logger) {
        this.log = logger;
        this.fields = new HashMap<>();
    }

    private PipeLogger(PipeLogger logger, Map<String, String> logFields) {
        this.log = logger.log;

        this.fields = new HashMap<>();
        this.fields.putAll(logger.fields);
        this.fields.putAll(logFields);
    }

    public PipeLogger withMessage(Message message) {

        Map<String, String> fields = new HashMap<>();

        fields.put("type", message.getType());
        fields.put("key", message.getKey());
        fields.put("contentType", message.getContentType());
        fields.put("offset", String.valueOf(message.getOffset()));

        return new PipeLogger(this, fields);
    }

    public PipeLogger withTypes(List<String> types) {

        if (!log.isDebugEnabled() || types == null) {
            return this;
        }

        Map<String, String> fields = Collections.singletonMap(
            "types",
            Arrays.toString(types.toArray())
        );

        return new PipeLogger(this, fields);
    }

    public void error(String where, String what, String why) {
        log(where, what, why, log::error);
    }

    public void error(String where, String what, Throwable why) {
        log(where, what, why, log::error);
    }

    public void info(String where, String what) {
        if (log.isInfoEnabled()) {
            log(where, what, log::info);
        }
    }

    public void info(String where, String what, Throwable why) {
        if (log.isInfoEnabled()) {
            log(where, what, why, log::info);
        }
    }

    public void debug(String where, String what) {
        if (log.isDebugEnabled()) {
            log(where, what, log::debug);
        }
    }

    public void debug(String where, String what, Throwable why) {
        if (log.isDebugEnabled()) {
            log(where, what, why, log::debug);
        }
    }

    private void log(String where, String what, Consumer<String> loggerFunc) {
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what);
        } finally {
            MDC.clear();
        }
    }

    private void log(String where, String what, Object why, BiConsumer<String, Object> loggerFunc) {
        try {
            fields.put("method", where);
            MDC.setContextMap(fields);
            loggerFunc.accept(what, why);
        } finally {
            MDC.clear();
        }
    }

    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }
}
