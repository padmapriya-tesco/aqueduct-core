import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.registry.model.NodeRegistry
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRegistry
import com.tesco.aqueduct.registry.model.TillStorage
import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.model.Till
import groovy.sql.Sql
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.HttpStatus
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.DriverManager
import java.time.Duration

import static io.restassured.RestAssured.given
import static io.restassured.RestAssured.when
import static org.hamcrest.Matchers.*

class NodeRegistryControllerV2IntegrationSpec extends Specification {
    static final String cloudPipeUrl = "http://cloud.pipe"
    static final String USERNAME = "username"
    static final String PASSWORD = "password"
    static final String USERNAME_TWO = "username-two"
    static final String PASSWORD_TWO = "password-two"

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    @ClassRule @Shared
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    DataSource dataSource
    NodeRegistry registry
    TillStorage mockTillStorage = Mock()

    def setupDatabase() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()

        dataSource.connection >> {
            DriverManager.getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))
        }

        sql.execute("""
            DROP TABLE IF EXISTS registry;
            
            CREATE TABLE registry(
            group_id VARCHAR PRIMARY KEY NOT NULL,
            entry JSON NOT NULL,
            version integer NOT NULL
            );
        """)

        registry = new PostgreSQLNodeRegistry(dataSource, new URL(cloudPipeUrl), Duration.ofDays(1))
    }

    void setup() {
        setupDatabase()
        context = ApplicationContext
            .build()
            .properties(
                // enabling security to prove that registry is accessible anyway
                parseYamlConfig(
                    """
                    micronaut.security.enabled: true
                    authentication:
                      users:
                        $USERNAME:
                          password: $PASSWORD
                          roles:
                            - REGISTRY_DELETE
                            - BOOTSTRAP_TILL
                        $USERNAME_TWO:
                          password: $PASSWORD_TWO
                    """
                )
            )
            .build()
            .registerSingleton(NodeRegistry, new PostgreSQLNodeRegistry(dataSource, new URL(cloudPipeUrl), Duration.ofDays(1)))
            .registerSingleton(MessageReader, Mock(MessageReader))
            .registerSingleton(TillStorage, mockTillStorage)
            .start()

        server = context.getBean(EmbeddedServer)

        RestAssured.port = server.port
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'expect unauthorized when not providing username and password to registry'(){
        given:
        server.start()

        expect:
        given()
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "status": "initialising",
                "following": ["$cloudPipeUrl"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def "Can post to registry"() {
        setup: "A started server"
        server.start()

        expect: "We can post info to the registry"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()

        given()
            .header("Authorization", "Basic $encodedCredentials")
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "status": "initialising",
                "following": ["$cloudPipeUrl"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(200)
            .body("", equalTo([cloudPipeUrl]))
    }

    def "Can get registry summary"() {
        setup: "A started server"
        server.start()

        expect: "We can get info from registry"
        given()
            .when()
            .get("/v2/registry")
            .then()
            .statusCode(200)
            .body(
                "root.offset", notNullValue(),
                "root.localUrl", notNullValue(),
                "root.status", equalTo("ok")
            )
    }

    def "Registered nodes are returned"() {
        given: "We register a node"
        server.start()
        registerNode(6735, "http://1.1.1.1:1234", 123, "status", ["http://x"])

        when: "we get summary"
        def request = when().get("/v2/registry")

        then:
        request.then()
            .statusCode(200)
            .body(
                "followers[0].group", equalTo("6735"),
                "followers[0].localUrl", equalTo("http://1.1.1.1:1234"),
                "followers[0].offset", equalTo("123"),
                "followers[0].status", equalTo("status"),
                "followers[0].following", contains("http://x"),
                "followers[0].requestedToFollow", contains("http://cloud.pipe")
            )
    }

    @Unroll
    def "Summary can filter by stores - #comment"() {
        given: "We register nodes from different groups"
        server.start()
        registerNode("a", "http://a")
        registerNode("b", "http://b")
        registerNode("c", "http://c")

        when: "we get summary"
        def request = given()
            .urlEncodingEnabled(false) // to disable changing ',' to %2Cc as this ',' is special character here, not part of the value
            .when().get("/v2/registry$query")

        then:
        request.then()
            .statusCode(200)
            .body("followers*.group", containsInAnyOrder(result.toArray()))

        where:
        query                         | result          | comment
        ""                            | ["a", "b", "c"] | "no filters"
        "?groups=b"                   | ["b"]           | "one store"
        "?groups=a&groups=c&groups=b" | ["a", "b", "c"] | "all stores, repeated param"
        "?groups=b&groups=c"          | ["b", "c"]      | "some stores, repeated param"

        //Below cases not currently supported, uncomment once they are supported !
        //"?groups=a,c,b"             | ["a", "b", "c"] | "all stores, comma separated"
        //"?groups=a,c"               | ["a", "c"]      | "some stores, comma separated"
    }

    def "deleting a single node from the registry"() {
        given: "We register two nodes"
        server.start()
        registerNode(1234, "http://1.1.1.1:1234", 123, "status", ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, "status", ["http://y"])

        when: "node is deleted"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        given()
            .header("Authorization", "Basic $encodedCredentials")
            .contentType("application/json")
            .when()
            .delete("/v2/registry/1234/1234|http://1.1.1.1:1234")
            .then()
            .statusCode(200)

        then: "node has been deleted, and other groups are unaffected"
        def request = when().get("/v2/registry")

        request.body().prettyPrint().contains("http://1.1.1.1:4321")
        !request.body().prettyPrint().contains("http://1.1.1.1:1234")

        request.then().statusCode(200)
    }

    def "deleting a node from the registry and rebalancing a group"() {
        given: "We register multiple nodes"
        server.start()
        registerNode(1234, "http://1.1.1.1:0001", 123, "status")
        registerNode(1234, "http://1.1.1.1:0002", 123, "status")
        registerNode(1234, "http://1.1.1.1:0003", 123, "status")
        registerNode(1234, "http://1.1.1.1:0004", 123, "status")
        registerNode(1234, "http://1.1.1.1:0005", 123, "status")

        when: "first node is deleted"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        given()
            .header("Authorization", "Basic $encodedCredentials")
            .contentType("application/json")
            .when()
            .delete("/v2/registry/1234/1234|http://1.1.1.1:0001")
            .then()
            .statusCode(200)

        then: "registry has been rebalanced"
        def request = when().get("/v2/registry")

        request.then().body(
            "followers[0].localUrl", equalTo("http://1.1.1.1:0002"),
            "followers[1].localUrl", equalTo("http://1.1.1.1:0003"),
            "followers[2].localUrl", equalTo("http://1.1.1.1:0004"),
            "followers[3].localUrl", equalTo("http://1.1.1.1:0005"),
            //following lists
            "followers[0].requestedToFollow", equalTo(["http://cloud.pipe"]),
            "followers[1].requestedToFollow", equalTo(["http://1.1.1.1:0002","http://cloud.pipe"]),
            "followers[2].requestedToFollow", equalTo(["http://1.1.1.1:0002","http://cloud.pipe"]),
            "followers[3].requestedToFollow", equalTo(["http://1.1.1.1:0003","http://1.1.1.1:0002","http://cloud.pipe"])
        )

        request.then().statusCode(200)
    }

    def "authenticated user without deletion role cannot delete from the database"() {
        given: "We register two nodes"
        server.start()
        registerNode(1234, "http://1.1.1.1:1234", 123, "status", ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, "status", ["http://y"])

        when: "node is deleted"
        def encodedCredentials = "${USERNAME_TWO}:${PASSWORD_TWO}".bytes.encodeBase64().toString()
        given()
                .header("Authorization", "Basic $encodedCredentials")
                .contentType("application/json")
                .when()
                .delete("/v2/registry/1234/1234|http://1.1.1.1:1234")
                .then()
                .statusCode(403)

        then: "registry is unaffected by request"
        def request = when().get("/v2/registry")

        request.body().prettyPrint().contains("http://1.1.1.1:4321")
        request.body().prettyPrint().contains("http://1.1.1.1:1234")

        request.then().statusCode(200)
    }

    def "anonymous user without deletion role cannot delete from the database"() {
        given: "We register two nodes"
        server.start()
        registerNode(1234, "http://1.1.1.1:1234", 123, "status", ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, "status", ["http://y"])

        when: "node is deleted"
        given()
                .contentType("application/json")
                .when()
                .delete("/v2/registry/1234/1234|http://1.1.1.1:1234")
                .then()
                .statusCode(401)

        then: "registry is unaffected by request"
        def request = when().get("/v2/registry")

        request.body().prettyPrint().contains("http://1.1.1.1:4321")
        request.body().prettyPrint().contains("http://1.1.1.1:1234")

        request.then().statusCode(200)
    }

    @Unroll
    def "when a bootstrap is requested, a bootstrap request is saved for that till"() {
        given: "A registry running"
        server.start()

        List<Till> called = new ArrayList<>()

        mockTillStorage.save(_) >> { Till t ->
            called.add(t)
        }

        when: "bootstrap is called"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()

        given()
            .contentType("application/json")
        .when()
            .header("Authorization", "Basic $encodedCredentials")
            .body("""{
                "tillHosts": ["0000", "1111", "2222"], 
                "bootstrapType": "$bootstrapString"
            }""")
            .post("/v2/registry/bootstrap")
        .then()
            .statusCode(statusCode)

        then: "till is saved"
        called.get(0).getBootstrap().getType() == bootstrapType
        called.get(0).getBootstrap().requestedDate != null
        called.get(0).getHostId() == "0000"

        called.get(1).getBootstrap().getType() == bootstrapType
        called.get(1).getBootstrap().requestedDate != null
        called.get(1).getHostId() == "1111"

        called.get(2).getBootstrap().getType() == bootstrapType
        called.get(2).getBootstrap().requestedDate != null
        called.get(2).getHostId() == "2222"

        where:
        bootstrapString     | statusCode | bootstrapType
        "PROVIDER"          | 200        | BootstrapType.PROVIDER
        "PIPE_AND_PROVIDER" | 200        | BootstrapType.PIPE_AND_PROVIDER
    }

    def "when bootstrap is called with invalid bootstrap type, a 400 is returned"() {
        given: "A registry running"
        server.start()

        when: "bootstrap is called"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()

        given()
            .contentType("application/json")
        .when()
            .header("Authorization", "Basic $encodedCredentials")
            .body("""{
                "tillHosts": ["0000", "1111", "2222"], 
                "bootstrapType": "INVALID"
             }""")
            .post("/v2/registry/bootstrap")
        .then()
            .statusCode(400)

        then: "till is not saved"
        0 * mockTillStorage.save(_)
    }

    private static void registerNode(group, url, offset=0, status="initialising", following=[cloudPipeUrl]) {
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        given()
            .contentType("application/json")
            .header("Authorization", "Basic $encodedCredentials")
            .body("""{
                "group": "$group",
                "localUrl": "$url",
                "offset": "$offset",
                "status": "$status",
                "following": ["${following.join('", "')}"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(200)
    }

    private static Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
