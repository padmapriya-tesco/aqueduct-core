package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.registry.NodeRegistryController;
import io.micronaut.runtime.Micronaut;
import org.slf4j.LoggerFactory;

public class PipeCloudServer {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeCloudServer.class));

    public static void main(String[] args) {

        LOG.info("Pipe cloud server", "server started");

        Micronaut.run(new Class[]{
            PipeReadController.class,
            PipeErrorHandler.class,
            NodeRegistryController.class
        });
    }
}
