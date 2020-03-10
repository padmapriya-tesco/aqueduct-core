package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.LocationResolver;
import com.tesco.aqueduct.pipe.api.TokenProvider;

import java.util.Collections;
import java.util.List;

public class CloudLocationResolver implements LocationResolver {

    private final TokenProvider tokenProvider;

    public CloudLocationResolver(TokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
    }

    @Override
    public List<String> resolve(String location) {
        tokenProvider.retrieveIdentityToken();
        return Collections.emptyList();
    }
}
