package com.tesco.aqueduct.pipe.identity.validator;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Flowable;

@Client("${authentication.identity.url}")
@Requires(property = "authentication.identity.url")
public interface IdentityTokenValidatorClient {

    @Post("${authentication.identity.validate.token.path}")
    Flowable<ValidateTokenResponse> validateToken(
        @Body ValidateTokenRequest identityRequest,
        @QueryValue(value="clientIdAndSecret") String clientIdAndSecret
    );
}
