package com.tesco.aqueduct.pipe.identity.issuer;

import com.tesco.aqueduct.pipe.api.IdentityToken;
import com.tesco.aqueduct.pipe.api.TokenProvider;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class IdentityIssueTokenProvider implements TokenProvider {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(IdentityIssueTokenProvider.class));

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
        try {
            identityToken = identityIssueTokenClient.retrieveIdentityToken(
                new IssueTokenRequest(
                    identityClientId,
                    identityClientSecret
                )
            );
        } catch(HttpClientResponseException exception) {
            LOG.error("retrieveIdentityToken", "Identity response error: ", exception);
            if (exception.getStatus().getCode() > 499) {
                throw new IdentityServiceUnavailableException("Unexpected error from Identity with status - " + exception.getStatus());
            } else {
                throw exception;
            }
        }

        return identityToken;
    }
}
