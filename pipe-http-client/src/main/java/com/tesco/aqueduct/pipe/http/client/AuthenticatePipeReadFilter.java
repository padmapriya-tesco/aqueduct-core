package com.tesco.aqueduct.pipe.http.client;

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
public class AuthenticatePipeReadFilter implements HttpClientFilter {

    private String username;
    private String password;

    public AuthenticatePipeReadFilter(
        @Property(name = "authentication.read-pipe.username") String username,
        @Property(name = "authentication.read-pipe.password") String password
    ) {
        this.username = username;
        this.password = password;
    }

    @Override
    public Publisher<? extends HttpResponse<?>> doFilter(MutableHttpRequest<?> request, ClientFilterChain chain) {
        return chain.proceed(request.basicAuth(username, password));
    }
}
