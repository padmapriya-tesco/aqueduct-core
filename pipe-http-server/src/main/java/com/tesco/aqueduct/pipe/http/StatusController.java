package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.metrics.Measure;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import lombok.Data;

import java.util.Collections;
import java.util.Map;

@Secured(SecurityRule.IS_ANONYMOUS)
@Controller
@Measure
public class StatusController {

    //this is used for healthchecking between pipes currently - will need switching out for new standard
    @Deprecated
    @Get("/pipe/status")
    Map status() {
        return Collections.emptyMap();
    }

    @Get("/pipe/_status")
    Status newStatus() {
        return new Status("ok", Version.getImplementationVersion());
    }

    @Data
    private static class Status {
        private final String status;
        private final String version;
    }
}
