package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.context.annotation.Requires;
import io.micronaut.security.authentication.*;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
@Requires(property = "authentication.users")
public class PipeReadAuthenticationProvider implements AuthenticationProvider {
    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PipeReadAuthenticationProvider.class));
    private final List<User> users;

    @Inject
    public PipeReadAuthenticationProvider(final List<User> users) {
        this.users = users;
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(final AuthenticationRequest authenticationRequest) {
        final Object identity  = authenticationRequest.getIdentity();
        final Object secret = authenticationRequest.getSecret();
        return Flowable.just(
            authenticate(identity, secret)
        );
    }

    private AuthenticationResponse authenticate(final Object username, final Object password) {
        int userCount = users.isEmpty() ? 0 : users.size();
        LOG.debug("authenticate", "Users: " + userCount);
        return users.stream()
            .filter(user -> user.isAuthenticated(username, password))
            .findAny()
            .map(User::toAuthenticationResponse)
            .orElse(new AuthenticationFailed());
    }
}
