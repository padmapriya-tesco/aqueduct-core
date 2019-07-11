package com.tesco.aqueduct.pipe.mock;

import com.tesco.aqueduct.pipe.http.PipeErrorHandler;
import com.tesco.aqueduct.pipe.http.PipeReadController;
import com.tesco.aqueduct.pipe.http.PipeWriteController;
import io.micronaut.runtime.Micronaut;

public class PipeMockServer {
    public static void main(final String[] args) {
        Micronaut.run(new Class[]{
            PipeWriteController.class,
            PipeReadController.class,
            PipeErrorHandler.class
        });
    }
}
