package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.MessageReader;
import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.registry.InMemoryNodeRegistry;
import com.tesco.aqueduct.registry.NodeRegistry;
import com.tesco.aqueduct.pipe.storage.PostgresqlStorage;
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

    @Singleton
    PostgresqlStorage bindPostgreSQL(
        @Value("${persistence.read.limit}") int limit,
        @Value("${persistence.read.retry-after}") int retryAfter,
        @Value("${persistence.read.max-batch-size}") int maxBatchSize,
        @Named("postgres") DataSource dataSource
    ) {
        return new PostgresqlStorage(dataSource, limit, retryAfter, maxBatchSize);
    }

    @Singleton @Named("local")
    @Measure
    MessageReader bindMessageReader(PostgresqlStorage postgresqlStorage){
        return postgresqlStorage;
    }

    @Singleton
    @Measure
    NodeRegistry bindNodeRegistry(
        @Value("${pipe.server.url}") URL selfUrl,
        @Value("${registry.mark-offline-after:1m}") Duration markAsOffline
    ) {
        return new InMemoryNodeRegistry(selfUrl, markAsOffline);
    }
}
