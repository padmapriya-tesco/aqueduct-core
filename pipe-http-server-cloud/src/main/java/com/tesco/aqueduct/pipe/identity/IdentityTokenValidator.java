package com.tesco.aqueduct.pipe.identity;

import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.async.annotation.SingleResult;
import io.micronaut.core.order.Ordered;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.token.validator.TokenValidator;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
@Requires(property = "identity.url")
public class IdentityTokenValidator implements TokenValidator {

    @Inject
    private IdentityTokenValidatorClient identityTokenValidatorClient;

    @Override
    @SingleResult
    @Cacheable("identity-cache")
    public Publisher<Authentication> validateToken(String token) {
        return identityTokenValidatorClient
            .validateToken(UUID.randomUUID().toString(), new ValidateTokenRequest(token))
            .filter(ValidateTokenResponse::isTokenValid)
            .map(ValidateTokenResponse::asAuthentication);
    }

    //lowest precedence chosen so it is used after others
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
