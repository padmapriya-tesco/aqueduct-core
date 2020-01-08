import com.tesco.aqueduct.pipe.identity.IdentityTokenValidator
import com.tesco.aqueduct.pipe.identity.IdentityTokenValidatorClient
import com.tesco.aqueduct.pipe.identity.TokenUser
import com.tesco.aqueduct.pipe.identity.ValidateTokenRequest
import com.tesco.aqueduct.pipe.identity.ValidateTokenResponse
import io.micronaut.security.authentication.Authentication
import io.reactivex.Flowable
import spock.lang.Specification
import spock.lang.Unroll

class IdentityTokenValidatorSpec extends Specification {

    private static final ValidateTokenResponse VALID_RESPONSE = new ValidateTokenResponse(ValidateTokenResponseSpec.USER_ID, ValidateTokenResponseSpec.VALID, ValidateTokenResponseSpec.VALID_CLAIMS)
    private static final ValidateTokenResponse INVALID_RESPONSE = new ValidateTokenResponse(ValidateTokenResponseSpec.USER_ID, ValidateTokenResponseSpec.INVALID, [])
    private static final String clientUid = ValidateTokenResponseSpec.USER_ID

    @Unroll
    def "Validate #description"() {
        given:
        TokenUser tokenUser = new TokenUser("name")
        tokenUser.clientId = clientUid
        def tokenValidator = new IdentityTokenValidator([tokenUser])
        def identityTokenValidatorClient = Mock(IdentityTokenValidatorClient) {
            validateToken(_ as String, _ as ValidateTokenRequest) >> Flowable.just(identityResponse)
        }

        tokenValidator.identityTokenValidatorClient = identityTokenValidatorClient

        when:
        def result = tokenValidator.validateToken("token") as Flowable

        then:
        predicate(result)

        where:
        identityResponse || predicate                                                                                     | description
        INVALID_RESPONSE || { Flowable r -> r.isEmpty().blockingGet() }                                                   | 'no authorisation if invalid identity response'
        VALID_RESPONSE   || { Flowable<Authentication> r -> r.blockingFirst().name == ValidateTokenResponseSpec.USER_ID } | 'authorised if valid identity response'
    }
}
