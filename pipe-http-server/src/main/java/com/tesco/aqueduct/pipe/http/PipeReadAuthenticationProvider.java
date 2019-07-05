package com.tesco.aqueduct.pipe.http;

import io.micronaut.context.annotation.Requires;
import io.micronaut.security.authentication.*;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
@Requires(property = "authentication.users")
public class PipeReadAuthenticationProvider implements AuthenticationProvider {

    private final List<User> users;

    @Inject
    public PipeReadAuthenticationProvider(List<User> users) {
        this.users = users;
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(AuthenticationRequest authenticationRequest) {
        Object identity  = authenticationRequest.getIdentity();
        Object secret = authenticationRequest.getSecret();

        return Flowable.just(
            authenticate(identity, secret)
        );
    }

    AuthenticationResponse authenticate(Object username, Object password) {
        return users.stream()
            .filter(user -> user.isAuthenticated(username, password))
            .findAny()
            .map(User::toAuthenticationResponse)
            .orElse(new AuthenticationFailed());
    }
}
