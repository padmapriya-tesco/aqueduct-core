package com.tesco.aqueduct.pipe.http;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ValidateTokenRequest {

    @JsonProperty("access_token")
    public final String accessToken;

    public ValidateTokenRequest(String accessToken) {
        this.accessToken = accessToken;
    }
}
