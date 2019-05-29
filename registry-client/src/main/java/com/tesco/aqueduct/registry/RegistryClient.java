package com.tesco.aqueduct.registry;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.Retryable;

import java.net.URL;
import java.util.List;

@Client("${pipe.http.client.url}")
@Requires(property = "pipe.http.register.retry.interval")
public interface RegistryClient {
    @Retryable(delay = "${pipe.http.register.retry.interval}")
    @Post(uri = "/registry")
    @Header(name="Accept-Encoding", value="gzip, deflate")
    List<URL> register(@Body Node node);
}
