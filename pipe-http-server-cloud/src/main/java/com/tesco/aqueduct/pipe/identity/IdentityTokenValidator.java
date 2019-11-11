package com.tesco.aqueduct.pipe.identity;

import io.micronaut.cache.annotation.Cacheable;
import io.micronaut.context.annotation.Property;
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
@Requires(property = "authentication.identity.url")
@Requires(property = "authentication.identity.clientId")
public class IdentityTokenValidator implements TokenValidator {

    private static String clientUid;

    @Inject
    private IdentityTokenValidatorClient identityTokenValidatorClient;

    public IdentityTokenValidator(@Property(name = "authentication.identity.clientId") String clientUid) {
        IdentityTokenValidator.clientUid = clientUid;
    }

    @Override
    @SingleResult
    @Cacheable("identity-cache")
    public Publisher<Authentication> validateToken(String token) {
        return identityTokenValidatorClient
            .validateToken(UUID.randomUUID().toString(), new ValidateTokenRequest(token))
            .filter(ValidateTokenResponse::isTokenValid)
            .map(ValidateTokenResponse::asAuthentication)
            .filter(IdentityTokenValidator::isClientUIDAuthorised);
    }

    private static Boolean isClientUIDAuthorised(Authentication authentication) {
        return authentication.getName().equals(clientUid);
    }

    //lowest precedence chosen so it is used after others
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
