package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.TokenProvider;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.ClientFilterChain;
import io.micronaut.http.filter.HttpClientFilter;
import org.reactivestreams.Publisher;

@Filter("${location.service.cluster.path}/**")
public class AuthenticateLocationServiceFilter implements HttpClientFilter {

    private final TokenProvider identityTokenProvider;

    public AuthenticateLocationServiceFilter(TokenProvider identityTokenProvider) {
        this.identityTokenProvider = identityTokenProvider;
    }

    @Override
    public Publisher<? extends HttpResponse<?>> doFilter(
        final MutableHttpRequest<?> request, final ClientFilterChain chain) {
        return chain.proceed(request.bearerAuth(identityTokenProvider.retrieveIdentityToken().getAccessToken()));
    }
}
