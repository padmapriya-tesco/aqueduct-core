package com.tesco.aqueduct.pipe.identity.issuer;

import com.tesco.aqueduct.pipe.api.IdentityToken;
import com.tesco.aqueduct.pipe.api.TokenProvider;
import io.micronaut.context.annotation.Property;

import java.util.UUID;


public class IdentityIssueTokenProvider implements TokenProvider {

    private final IdentityIssueTokenClient identityIssueTokenClient;
    private final String identityClientId;
    private final String identityClientSecret;

    public IdentityIssueTokenProvider(
        IdentityIssueTokenClient identityIssueTokenClient,
        @Property(name = "authentication.identity.client.id") String identityClientId,
        @Property(name = "authentication.identity.client.secret") String identityClientSecret
    ) {
        this.identityIssueTokenClient = identityIssueTokenClient;
        this.identityClientId = identityClientId;
        this.identityClientSecret = identityClientSecret;
    }

    // Look into expiring Identity token cache here by
    // potentially calling cache expiry method on identity client if the token has expired
    @Override
    public IdentityToken retrieveIdentityToken() {
        return identityIssueTokenClient.retrieveIdentityToken(
            UUID.randomUUID().toString(),
            new IssueTokenRequest(
                identityClientId,
                identityClientSecret
            )
        );
    }
}
