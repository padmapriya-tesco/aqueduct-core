package com.tesco.aqueduct.pipe.identity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tesco.aqueduct.pipe.api.IdentityToken;

public class CloudIdentityToken implements IdentityToken {

    private final String accessToken;
    private long tokenExpiry;

    @JsonCreator
    public CloudIdentityToken(
            @JsonProperty("access_token") final String accessToken,
            @JsonProperty("expires_in") long tokenExpiry) {
        this.accessToken = accessToken;
        this.tokenExpiry = tokenExpiry;
    }

    @Override
    public String getAccessToken() {
        return this.accessToken;
    }
}
