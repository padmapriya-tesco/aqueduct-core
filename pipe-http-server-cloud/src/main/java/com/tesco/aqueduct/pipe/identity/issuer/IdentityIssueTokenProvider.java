package com.tesco.aqueduct.pipe.identity.issuer;

import com.tesco.aqueduct.pipe.api.IdentityToken;
import com.tesco.aqueduct.pipe.api.TokenProvider;

import java.util.UUID;

public class IdentityIssueTokenProvider implements TokenProvider {

    private final IdentityIssueTokenClient identityIssueTokenClient;
    private final String identityClientId;
    private final String identityClientSecret;
    private IdentityToken identityToken;

    public IdentityIssueTokenProvider(
        IdentityIssueTokenClient identityIssueTokenClient,
        String identityClientId,
        String identityClientSecret
    ) {
        this.identityIssueTokenClient = identityIssueTokenClient;
        this.identityClientId = identityClientId;
        this.identityClientSecret = identityClientSecret;
    }

    @Override
    public IdentityToken retrieveIdentityToken() {
        if (identityToken != null && !identityToken.isTokenExpired()) {
            return identityToken;
        }

        identityToken = identityIssueTokenClient.retrieveIdentityToken(
            UUID.randomUUID().toString(),
            new IssueTokenRequest(
                identityClientId,
                identityClientSecret
            )
        );

        return identityToken;
    }
}
