package com.tesco.aqueduct.pipe.identity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.DefaultAuthentication;

import java.util.Collection;
import java.util.Collections;

public class ValidateTokenResponse {
    private static final int SERVICE_CONFIDENCE_LEVEL = 12;

    private final String userId;
    private final String status;
    private final Collection<Claim> claims;

    private int confidenceLevel = 0;

    static class Claim {
        private static final String CONFIDENCE_LEVEL_CLAIM = "http://schemas.tesco.com/ws/2011/12/identity/claims/confidencelevel";

        private String claimType, value;

        @JsonCreator
        Claim(
            @JsonProperty(value = "claimType", required = true) String claimType,
            @JsonProperty(value = "value", required = true) String value
        ) {
            this.claimType = claimType;
            this.value = value;
        }

        boolean isForConfidenceLevel(){
            return this.claimType.equals(CONFIDENCE_LEVEL_CLAIM);
        }
    }

    @JsonCreator
    ValidateTokenResponse(
        @JsonProperty(value = "UserId") String userId,
        @JsonProperty(value = "Status", required = true) String status,
        @JsonProperty(value = "Claims") Collection<Claim> claims
    ) {
        this.userId = userId;
        this.status = status;
        this.claims = claims;

        if (claims != null) {
            this.confidenceLevel = determineConfidenceLevel();
        }
    }

    private int determineConfidenceLevel() {
        return claims.stream()
            .filter(Claim::isForConfidenceLevel)
            .mapToInt(claim -> Integer.parseInt(claim.value))
            .findFirst()
            .orElse(0);
    }

    boolean isTokenValid() {
        return status.equals("VALID") && confidenceLevel == SERVICE_CONFIDENCE_LEVEL;
    }

    Authentication asAuthentication(){
        return new DefaultAuthentication(this.userId, Collections.emptyMap());
    }
}
