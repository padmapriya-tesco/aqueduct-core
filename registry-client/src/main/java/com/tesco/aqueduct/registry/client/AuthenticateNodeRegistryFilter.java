package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.pipe.api.TokenProvider;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.ClientFilterChain;
import io.micronaut.http.filter.HttpClientFilter;
import org.reactivestreams.Publisher;

@Filter("/**/registry")
public class AuthenticateNodeRegistryFilter implements HttpClientFilter {

    private final TokenProvider tokenProvider;

    public AuthenticateNodeRegistryFilter(
            TokenProvider tokenProvider
    ) {
        this.tokenProvider = tokenProvider;
    }

    @Override
    public Publisher<? extends HttpResponse<?>> doFilter(final MutableHttpRequest<?> request, final ClientFilterChain chain) {
        return chain.proceed(request.bearerAuth(tokenProvider.retrieveIdentityToken().getAccessToken()));
    }
}
