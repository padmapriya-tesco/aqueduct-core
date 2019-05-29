import com.tesco.aqueduct.pipe.api.MessageWriter
import com.tesco.aqueduct.pipe.storage.InMemoryStorage
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import static io.restassured.RestAssured.given

class PipeWriteControllerSpec extends Specification {

    InMemoryStorage storage = new InMemoryStorage(1000, 10000)

    @Shared
    String singleMessage = '''[
        {
            "type" : "type1",
            "key": "x",
            "offset": "1",
            "created": "2018-10-01T13:45:00Z",
            "tags": {"a": "tag"},
            "data": "{\\"some\\": \\"data\\"}"
        }
    ]'''

    @Unroll
    void "post #bodyComment to #path and get #statusCode back"() {
        given:
        def context = ApplicationContext.run()
        context.registerSingleton(MessageWriter, storage)
        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        when:
        def response =
            given()
                .port(server.port)
                .body(body)
                .when()
                .header("Content-Type", "application/json")
                .post(path)

        then:
        response.then().statusCode(statusCode)

        cleanup:
        context.close()

        where:
        path             | body          | bodyComment      | statusCode
        "/"              | "[]"          | "empty list"     | 404
        "/referenceData" | "[]"          | "empty list"     | 404
        "/pipe/type1"    | ""            | "no list"        | 404
        "/pipe"          | "[]"          | "empty list"     | 200
        "/pipe"          | singleMessage | "single message" | 200
    }
}
