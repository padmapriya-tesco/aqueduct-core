import com.tesco.aqueduct.pipe.identity.ValidateTokenResponse
import spock.lang.Specification
import spock.lang.Unroll

class ValidateTokenResponseSpec extends Specification {

    static final def USER_ID = "userId"
    static final def INVALID = "INVALID"
    static final def VALID = "VALID"
    static final def INVALID_CLAIMS = [new ValidateTokenResponse.Claim(ValidateTokenResponse.Claim.CONFIDENCE_LEVEL_CLAIM, "2")]
    static final def VALID_CLAIMS = [new ValidateTokenResponse.Claim(ValidateTokenResponse.Claim.CONFIDENCE_LEVEL_CLAIM, "12")]

    @Unroll
    def "Verify token is #description"() {
        given:
        def response = new ValidateTokenResponse(USER_ID, status, claims)

        expect:
        expected == response.isTokenValid()

        where:
        status  | claims         || expected | description
        INVALID | []             || false    | "invalid when status is invalid and no claims are present"
        VALID   | INVALID_CLAIMS || false    | "invalid when status is valid but claims are invalid"
        VALID   | VALID_CLAIMS   || true     | "valid when both status and claims are valid"
    }
}
