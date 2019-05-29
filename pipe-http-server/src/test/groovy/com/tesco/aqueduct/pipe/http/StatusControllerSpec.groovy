package com.tesco.aqueduct.pipe.http

import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import static io.restassured.RestAssured.when
import static org.hamcrest.Matchers.equalTo

class StatusControllerSpec extends Specification {
    static final String USERNAME = "username"
    static final String PASSWORD = "password"

    static final String RUNSCOPE_USERNAME = "runscope-username"
    static final String RUNSCOPE_PASSWORD = "runscope-password"

    @Shared @AutoCleanup ApplicationContext context
    @Shared @AutoCleanup EmbeddedServer server


    void setupSpec() {
        context = ApplicationContext
            .build()
            .properties(
                "micronaut.security.enabled": true,
                "authentication.read-pipe.username": USERNAME,
                "authentication.read-pipe.password": PASSWORD,
                "authentication.read-pipe.runscope-username": RUNSCOPE_USERNAME,
                "authentication.read-pipe.runscope-password": RUNSCOPE_PASSWORD
            )
            .mainClass(StatusController)
            .build()

        context.start()

        server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'a call to the status endpoint returns 200 without needing to authenticate as there is nothing to restrict yet'() {
        expect: "the status code returned by the pipe status endpoint is OK"
        RestAssured.get("/pipe/status")
            .then()
            .statusCode(HttpStatus.OK.code)
    }

    def "pipe status contains version in it"() {
        expect:
        when()
            .get("/pipe/_status")
            .then()
            .statusCode(200)
            .body("version", equalTo(Version.getImplementationVersion()))
    }

    def "status endpoint returns 200 when pipe is online"() {
        when: "call to /pipe/status"
        def response = RestAssured.get("/pipe/status")

        then:
        response.statusCode() == 200
        response.contentType() == "application/json"
    }
}
