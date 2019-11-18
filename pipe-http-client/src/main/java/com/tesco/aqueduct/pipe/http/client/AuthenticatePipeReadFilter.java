package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.TokenProvider;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.ClientFilterChain;
import io.micronaut.http.filter.HttpClientFilter;
import org.reactivestreams.Publisher;

@Filter(serviceId = "pipe")
@Requires(property = "authentication.read-pipe.username")
@Requires(property = "authentication.read-pipe.password")
@Requires(property = "pipe.http.client.url")
public class AuthenticatePipeReadFilter implements HttpClientFilter {

    private final String username;
    private final String password;
    private final String pipeCloudUri;
    private final TokenProvider tokenProvider;

    public AuthenticatePipeReadFilter(
        @Property(name = "authentication.read-pipe.username") final String username,
        @Property(name = "authentication.read-pipe.password") final String password,
        @Property(name = "pipe.http.client.url") final String pipeCloudUri,
        TokenProvider tokenProvider
    ) {
        this.username = username;
        this.password = password;
        this.pipeCloudUri = pipeCloudUri;
        this.tokenProvider = tokenProvider;
    }

    @Override
    public Publisher<? extends HttpResponse<?>> doFilter(final MutableHttpRequest<?> request, final ClientFilterChain chain) {
        if (request.getUri().toString().contains(pipeCloudUri)) {
            return chain.proceed(request.bearerAuth(tokenProvider.retrieveIdentityToken().getAccessToken()));
        }
        return chain.proceed(request.basicAuth(username, password));
    }
}
