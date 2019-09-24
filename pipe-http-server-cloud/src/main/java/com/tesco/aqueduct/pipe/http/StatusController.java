package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import lombok.Data;

@Secured(SecurityRule.IS_ANONYMOUS)
@Controller
@Measure
public class StatusController {

    @Get("/_status")
    Status status() {
        return new Status("ok", Version.getImplementationVersion());
    }

    @Data
    private static class Status {
        private final String status;
        private final String version;
    }
}
