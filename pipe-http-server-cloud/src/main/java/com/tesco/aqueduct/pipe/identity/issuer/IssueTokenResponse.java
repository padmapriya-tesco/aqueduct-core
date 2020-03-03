package com.tesco.aqueduct.pipe.identity.issuer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tesco.aqueduct.pipe.api.IdentityToken;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IssueTokenResponse implements IdentityToken {

    private final String accessToken;
    private final long tokenExpiry;

    @JsonCreator
    public IssueTokenResponse(
        @JsonProperty("access_token") final String accessToken,
        @JsonProperty("expires_in") long tokenExpiry
    ) {
        this.accessToken = accessToken;
        this.tokenExpiry = tokenExpiry;
    }

    @Override
    public String getAccessToken() {
        return this.accessToken;
    }

    @Override
    public long getTokenExpiry() {
        return tokenExpiry;
    }
}
