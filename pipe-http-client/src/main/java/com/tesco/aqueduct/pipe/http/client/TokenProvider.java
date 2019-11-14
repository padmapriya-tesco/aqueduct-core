package com.tesco.aqueduct.pipe.http.client;

public interface TokenProvider {
    IdentityTokenResponse retrieveIdentityToken();
}
