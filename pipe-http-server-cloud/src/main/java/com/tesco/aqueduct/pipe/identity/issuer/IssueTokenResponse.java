package com.tesco.aqueduct.pipe.identity.issuer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tesco.aqueduct.pipe.api.IdentityToken;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IssueTokenResponse implements IdentityToken {

    private final String accessToken;
    private final long expiresAt;

    @JsonCreator
    public IssueTokenResponse(
        @JsonProperty("access_token") final String accessToken,
        @JsonProperty("expires_in") long tokenExpiresInSec
    ) {
        this.accessToken = accessToken;
        this.expiresAt = System.currentTimeMillis() + (tokenExpiresInSec * 1000);
    }

    @Override
    public String getAccessToken() {
        return this.accessToken;
    }

    @Override
    public boolean isTokenExpired() {
        return System.currentTimeMillis() > expiresAt;
    }
}
