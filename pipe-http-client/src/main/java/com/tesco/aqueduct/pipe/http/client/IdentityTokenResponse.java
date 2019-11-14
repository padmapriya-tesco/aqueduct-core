package com.tesco.aqueduct.pipe.http.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class IdentityTokenResponse {

    private final String accessToken;

    @JsonCreator
    IdentityTokenResponse(
            @JsonProperty(value = "access_token") String accessToken
    ) {
        this.accessToken = accessToken;
    }
}