package com.tesco.aqueduct.pipe.http;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.security.authentication.*;
import io.reactivex.Flowable;
import lombok.Data;
import org.reactivestreams.Publisher;

import javax.inject.Singleton;
import java.util.List;

@Singleton
@Requires(property = "authentication.users")
public class PipeReadAuthenticationProvider implements AuthenticationProvider {

    private final List<User> users;

    public PipeReadAuthenticationProvider(@Property(name = "authentication.users") List<User> users) {
        this.users = users;
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(AuthenticationRequest authenticationRequest) {
        Object identity  = authenticationRequest.getIdentity();
        Object secret = authenticationRequest.getSecret();

        return Flowable.just(
            users.stream()
                .filter(user -> user.isAuthenticated(identity, secret))
                .findAny()
                .<AuthenticationResponse>map(user -> new UserDetails(user.getUsername(), user.getRoles()))
                .orElse(new AuthenticationFailed())
        );
    }

    @Data
    public static class User {
        private final String username;
        private final String password;
        private final List<String> roles;

        boolean isAuthenticated(Object identity, Object secret) {
            return username.equals(identity) && password.equals(secret);
        }
    }
}
