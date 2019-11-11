package com.tesco.aqueduct.pipe.identity;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Flowable;

@Client("identity.url")
public interface IdentityTokenValidatorClient {

    @Post("identity.validate.token.path")
    Flowable<ValidateTokenResponse> validateToken(
        @Header("TraceId") String traceId,
        @Body ValidateTokenRequest identityRequest
    );
}
