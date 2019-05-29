package com.tesco.aqueduct.pipe.http;

import io.micronaut.context.annotation.Property;
import io.micronaut.security.authentication.*;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import javax.inject.Singleton;
import java.util.ArrayList;

@Singleton
public class PipeReadAuthenticationProvider implements AuthenticationProvider {

    private final String username;
    private final String password;
    private final String runscopeUsername;
    private final String runscopePassword;

    public PipeReadAuthenticationProvider(
        @Property(name = "authentication.read-pipe.username") String username,
        @Property(name = "authentication.read-pipe.password") String password,
        @Property(name = "authentication.read-pipe.runscope-username") String runscopeUsername,
        @Property(name = "authentication.read-pipe.runscope-password") String runscopePassword
    ) {
        this.username = username;
        this.password = password;
        this.runscopeUsername = runscopeUsername;
        this.runscopePassword = runscopePassword;
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(AuthenticationRequest authenticationRequest) {
        Object identity  = authenticationRequest.getIdentity();
        Object secret = authenticationRequest.getSecret();

        if (validateIdentityAndSecret(identity, secret)) {
            return Flowable.just(new UserDetails((String) authenticationRequest.getIdentity(), new ArrayList<>()));
        }
        return Flowable.just(new AuthenticationFailed());
    }

    private boolean validateIdentityAndSecret(Object identity, Object secret) {
        return (identity.equals(this.username) && secret.equals(this.password)) ||
               (identity.equals(this.runscopeUsername) && secret.equals(this.runscopePassword));
    }
}
