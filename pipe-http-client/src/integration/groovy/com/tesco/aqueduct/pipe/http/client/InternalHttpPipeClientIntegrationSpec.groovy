package com.tesco.aqueduct.pipe.http.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.api.PipeStateResponse
import com.tesco.aqueduct.registry.PipeLoadBalancer
import com.tesco.aqueduct.registry.SelfRegistrationTask
import io.micronaut.context.ApplicationContext
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZonedDateTime

class InternalHttpPipeClientIntegrationSpec extends Specification {

    @Shared @AutoCleanup ErsatzServer server
    @Shared @AutoCleanup("stop") ApplicationContext context

    PipeLoadBalancer loadBalancer
    InternalHttpPipeClient client

    def setupSpec() {
        server = new ErsatzServer()
        server.start()

        context = ApplicationContext
            .build()
            .properties(
                "pipe.http.latest-offset.attempts": 1,
                "pipe.http.latest-offset.delay": "1s",
                "pipe.http.client.url": server.getHttpUrl()
            )
            .build()
            .registerSingleton(SelfRegistrationTask, Mock(SelfRegistrationTask))
            .start()
    }

    def setup() {
        client = context.getBean(InternalHttpPipeClient)

        loadBalancer = context.getBean(PipeLoadBalancer)
        loadBalancer.update([new URL(server.getHttpUrl())])
    }

    def cleanup() {
        server.clearExpectations()
    }

    @Unroll
    def "Client is calling correct link with proper parameters"() {
        given:
        server.expectations {
            get("/pipe/$offset") {
                header('Accept', 'application/json')
                header('Accept-Encoding', 'gzip, deflate')
                queries(type: type)
                called(1)

                responder {
                    header("Retry-After", "1")
                    contentType('application/json')
                    body("""[
                        {
                            "type": "$type",
                            "key": "x",
                            "contentType": "$ct",
                            "offset": $offset,
                            "created": "2018-10-01T13:45:00Z", 
                            "data": "{ \\"valid\\": \\"json\\" }"
                        }
                    ]""")
                }
            }
        }

        when:
        def messages = client.httpRead([type], offset).body()

        def expectedMessage = new Message(
            type,
            "x",
            ct,
            offset,
            ZonedDateTime.parse("2018-10-01T13:45:00Z"),
            '{ "valid": "json" }'
        )
        then:
        server.verify()
        messages[0] == expectedMessage

        where:
        type    | ct             | offset
        "type1" | "contentType1" | 123
        "type2" | "contentType2" | 123
        "type1" | "contentType3" | 111
        "type2" | "contentType4" | 123
    }

    def "can get latest offset"(){
        given:
        server.expectations {
            get("/pipe/offset/latest") {
                called(1)

                responder {
                    contentType('application/json')
                    body('123')
                }
            }
        }

        when:
        def latest = client.getLatestOffsetMatching([])
        then:
        server.verify()
        latest == 123
    }

    def "can get latest offset with tags"(){
        given:
        server.expectations {
            get("/pipe/offset/latest") {
                called(1)
                query("type","a,b,c")

                responder {
                    contentType('application/json')
                    body('100')
                }
            }
        }

        when:
        def latest = client.getLatestOffsetMatching(["a", "b", "c"])

        then:
        server.verify()
        latest == 100
    }

    def "can get pipe state"(){
        given:
        server.expectations {
            get("/pipe/state") {
                called(1)
                query("type", "a")

                responder {
                    contentType('application/json')
                    body("""{"isUpToDate":true,"offset":"1000"}""")
                }
            }
        }

        when:
        def state = client.getPipeState(["a"])

        then:
        server.verify()
        state == new PipeStateResponse(true, 1000)
    }
}
