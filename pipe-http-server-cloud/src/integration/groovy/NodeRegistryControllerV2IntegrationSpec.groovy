import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.MessageReader
import com.tesco.aqueduct.registry.model.NodeRegistry
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRegistry
import com.tesco.aqueduct.registry.model.TillStorage
import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.postgres.PostgreSQLTillStorage
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
    private static final String CLOUD_PIPE_URL = "http://cloud.pipe"
    private static final String USERNAME = "username"
    private static final String PASSWORD = "password"
    private static final String USERNAME_TWO = "username-two"
    private static final String PASSWORD_TWO = "password-two"
    private static final int SERVER_TIMEOUT_MS = 5000
    private static final int SERVER_SLEEP_TIME_MS = 500

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    @ClassRule @Shared
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql
    DataSource dataSource
    NodeRegistry registry
    TillStorage tillStorage

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

        sql.execute("""
            DROP TABLE IF EXISTS tills;
            
            CREATE TABLE tills(
            host_id VARCHAR PRIMARY KEY NOT NULL,
            bootstrap_requested timestamp NOT NULL,
            bootstrap_type VARCHAR NOT NULL,
            bootstrap_received timestamp
            );
        """)

        tillStorage = new PostgreSQLTillStorage(dataSource)
        registry = new PostgreSQLNodeRegistry(dataSource, new URL(CLOUD_PIPE_URL), Duration.ofDays(1))
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
                    micronaut.server.port: -1
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
            .registerSingleton(NodeRegistry, registry)
            .registerSingleton(MessageReader, Mock(MessageReader))
            .registerSingleton(TillStorage, tillStorage)
            .start()

        server = context.getBean(EmbeddedServer)

        RestAssured.port = server.port
        server.start()
        def time = 0
        while (!server.isRunning() && time < SERVER_TIMEOUT_MS) {
            println("Server not yet running...")
            sleep SERVER_SLEEP_TIME_MS
            time += SERVER_SLEEP_TIME_MS
        }
        println("Test setup complete")
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'expect unauthorized when not providing username and password to registry'(){
        expect:
        given()
            .contentType("application/json")
            .body("""{
                "group": "6735",
                "localUrl": "http://localhost:8080",
                "offset": "123",
                "status": "initialising",
                "following": ["$CLOUD_PIPE_URL"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def "Can post to registry"() {
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
                "following": ["$CLOUD_PIPE_URL"]
            }""")
        .when()
            .post("/v2/registry")
        .then()
            .statusCode(200)
            .body(
                "bootstrapType", equalTo("NONE"),
                "requestedToFollow", contains(CLOUD_PIPE_URL)
            )
    }

    def "Can get registry summary"() {
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
        registerNode(1234, "http://1.1.1.1:1234", 123, "status", ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, "status", ["http://y"])

        when: "node is deleted"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        given()
            .header("Authorization", "Basic $encodedCredentials")
            .contentType("application/json")
            .when()
            .delete("/v2/registry/1234/1.1.1.1")
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
        registerNode(1234, "http://1.1.1.1:0001", 123, "status")
        registerNode(1234, "http://1.1.1.2:0002", 123, "status")
        registerNode(1234, "http://1.1.1.3:0003", 123, "status")
        registerNode(1234, "http://1.1.1.4:0004", 123, "status")
        registerNode(1234, "http://1.1.1.5:0005", 123, "status")

        when: "first node is deleted"
        def encodedCredentials = "${USERNAME}:${PASSWORD}".bytes.encodeBase64().toString()
        given()
            .header("Authorization", "Basic $encodedCredentials")
            .contentType("application/json")
            .when()
            .delete("/v2/registry/1234/1.1.1.1")
            .then()
            .statusCode(200)

        then: "registry has been rebalanced"
        def request = when().get("/v2/registry")

        request.then().body(
            "followers[0].localUrl", equalTo("http://1.1.1.2:0002"),
            "followers[1].localUrl", equalTo("http://1.1.1.3:0003"),
            "followers[2].localUrl", equalTo("http://1.1.1.4:0004"),
            "followers[3].localUrl", equalTo("http://1.1.1.5:0005"),
            //following lists
            "followers[0].requestedToFollow", equalTo(["http://cloud.pipe"]),
            "followers[1].requestedToFollow", equalTo(["http://1.1.1.2:0002","http://cloud.pipe"]),
            "followers[2].requestedToFollow", equalTo(["http://1.1.1.2:0002","http://cloud.pipe"]),
            "followers[3].requestedToFollow", equalTo(["http://1.1.1.3:0003","http://1.1.1.2:0002","http://cloud.pipe"])
        )

        request.then().statusCode(200)
    }

    def "authenticated user without deletion role cannot delete from the database"() {
        given: "We register two nodes"
        registerNode(1234, "http://1.1.1.1:1234", 123, "status", ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, "status", ["http://y"])

        when: "node is deleted"
        def encodedCredentials = "${USERNAME_TWO}:${PASSWORD_TWO}".bytes.encodeBase64().toString()
        given()
                .header("Authorization", "Basic $encodedCredentials")
                .contentType("application/json")
                .when()
                .delete("/v2/registry/1234/1.1.1.1")
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
        registerNode(1234, "http://1.1.1.1:1234", 123, "status", ["http://x"])
        registerNode(4321, "http://1.1.1.1:4321", 123, "status", ["http://y"])

        when: "node is deleted"
        given()
                .contentType("application/json")
                .when()
                .delete("/v2/registry/1234/1.1.1.1")
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
        def rows = sql.rows("SELECT * FROM tills;")

        rows.get(0).getProperty("host_id") == "0000"
        rows.get(0).getProperty("bootstrap_requested") != null
        rows.get(0).getProperty("bootstrap_type") == bootstrapType
        rows.get(0).getProperty("bootstrap_received") == null

        rows.get(1).getProperty("host_id") == "1111"
        rows.get(1).getProperty("bootstrap_requested") != null
        rows.get(1).getProperty("bootstrap_type") == bootstrapType
        rows.get(1).getProperty("bootstrap_received") == null

        rows.get(2).getProperty("host_id") == "2222"
        rows.get(2).getProperty("bootstrap_requested") != null
        rows.get(2).getProperty("bootstrap_type") == bootstrapType
        rows.get(2).getProperty("bootstrap_received") == null

        where:
        bootstrapString     | statusCode | bootstrapType
        "PROVIDER"          | 200        | BootstrapType.PROVIDER.toString()
        "PIPE_AND_PROVIDER" | 200        | BootstrapType.PIPE_AND_PROVIDER.toString()
    }

    def "when bootstrap is called with invalid bootstrap type, a 400 is returned"() {
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
        def rows = sql.rows("SELECT * FROM tills;")
        rows.size() == 0
    }

    private static void registerNode(group, url, offset=0, status="initialising", following=[CLOUD_PIPE_URL]) {
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
