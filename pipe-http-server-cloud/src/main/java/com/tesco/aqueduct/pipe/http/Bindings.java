package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.registry.model.NodeRegistry;
import com.tesco.aqueduct.pipe.storage.PostgresqlStorage;
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRegistry;
import com.tesco.aqueduct.registry.postgres.PostgreSQLTillStorage;
import com.tesco.aqueduct.registry.model.TillStorage;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.net.URL;
import java.time.Duration;

@Factory
@Singleton
public class Bindings {

    // This provides MessageReader as it implements it
    @Singleton @Named("local")
    PostgresqlStorage bindPostgreSQL(
        @Value("${persistence.read.limit}") final int limit,
        @Value("${persistence.read.retry-after}") final int retryAfter,
        @Value("${persistence.read.max-batch-size}") final int maxBatchSize,
        @Named("postgres") final DataSource dataSource
    ) {
        return new PostgresqlStorage(dataSource, limit, retryAfter, maxBatchSize);
    }

    @Singleton
    @Measure
    NodeRegistry bindNodeRegistry(
        @Named("postgres") final DataSource dataSource,
        @Value("${pipe.server.url}") final URL selfUrl,
        @Value("${registry.mark-offline-after:1m}") final Duration markAsOffline
    ) {
        return new PostgreSQLNodeRegistry(dataSource, selfUrl, markAsOffline);
    }

    @Singleton
    @Measure
    TillStorage bindTillStorage(@Named("postgres") final DataSource dataSource) {
        return new PostgreSQLTillStorage(dataSource);
    }

    @Singleton
    PipeStateProvider bindPipeStateProvider() {
        return new CloudPipeStateProvider();
    }
}
