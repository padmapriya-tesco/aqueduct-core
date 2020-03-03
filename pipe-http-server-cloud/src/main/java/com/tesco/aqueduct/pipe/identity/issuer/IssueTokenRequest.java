package com.tesco.aqueduct.pipe.identity.issuer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class IssueTokenRequest {
    @JsonProperty("grant_type")
    private final String grantType = "client_credentials";
    @JsonProperty("scope")
    private final String scope = "internal public";
    @JsonProperty("confidence_level")
    private final int confidenceLevel = 12;
    @JsonProperty("client_id")
    private final String clientId;
    @JsonProperty("client_secret")
    private final String clientSecret;

    @JsonCreator
    public IssueTokenRequest(
        final String clientId,
        final String clientSecret
    ) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }
}
