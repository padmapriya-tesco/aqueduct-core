package com.tesco.aqueduct.pipe.identity.issuer;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.Retryable;

@Client("${authentication.identity.url}")
@Header(name = "Content-Type", value = MediaType.APPLICATION_JSON)
public interface IdentityIssueTokenClient {

    @Post(value = "${authentication.identity.issue.token.path}", consumes = "application/vnd.tesco.identity.tokenresponse+json")
    @Retryable(attempts = "${authentication.identity.attempts}", delay = "${authentication.identity.delay}")
    IssueTokenResponse retrieveIdentityToken(
        @Header("TraceId") String traceId,
        @Body IssueTokenRequest issueTokenRequest
    );
}
