package com.tesco.aqueduct.pipe.api;

public interface IdentityToken {
    String getAccessToken();
    boolean isTokenExpired();
}