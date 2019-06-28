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

    @Get("/pipe/{offset}{?type}")
    @Consumes
    @Header(name="Accept-Encoding", value="gzip, deflate")
    HttpResponse<List<Message>> httpRead(
            @Nullable List<String> type,
            long offset
    );

    @Get("/pipe/offset/latest{?type}")
    @Header(name="Accept-Encoding", value="gzip, deflate")
    @Retryable(attempts = "${pipe.http.latest-offset.attempts}", delay = "${pipe.http.latest-offset.delay}")
    long getLatestOffsetMatching(@Nullable  List<String> type);
}

// For usage of URI templates see
// https://github.com/micronaut-projects/micronaut-core/blob/master/http/src/test/groovy/io/micronaut/http/uri/UriTemplateSpec.groovy