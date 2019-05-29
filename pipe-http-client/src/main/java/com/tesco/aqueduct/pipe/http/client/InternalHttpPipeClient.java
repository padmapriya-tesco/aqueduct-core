package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.pipe.api.Message;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.retry.annotation.Retryable;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Retryable(attempts = "3")
@Client(id = "pipe")
public interface InternalHttpPipeClient {

    @Get("/pipe/{offset}{?tags*}")
    @Consumes
    @Header(name="Accept-Encoding", value="gzip, deflate")
    HttpResponse<List<Message>> httpRead(
            @Nullable Map<String, List<String>> tags,
            long offset
    );

    @Get("/pipe/offset/latest{?tags*}")
    @Header(name="Accept-Encoding", value="gzip, deflate")
    @Retryable(attempts = "${pipe.http.latest-offset.attempts}", delay = "${pipe.http.latest-offset.delay}")
    long getLatestOffsetMatching(Map<String, List<String>> tags);
}

// {?tags*) will inline parameters, i.e. [a:[1,2]] will be ?a=1&a2, there is no way in micronaut to
// serialize parameters in a=1,2 for a composite, but there is for top level fields with {list}, see:
// https://github.com/micronaut-projects/micronaut-core/blob/master/http/src/test/groovy/io/micronaut/http/uri/UriTemplateSpec.groovy