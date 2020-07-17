import com.tesco.aqueduct.pipe.identity.validator.IdentityTokenValidator
import com.tesco.aqueduct.pipe.identity.validator.IdentityTokenValidatorClient
import com.tesco.aqueduct.pipe.identity.validator.TokenUser
import com.tesco.aqueduct.pipe.identity.validator.ValidateTokenRequest
import com.tesco.aqueduct.pipe.identity.validator.ValidateTokenResponse
import io.micronaut.security.authentication.Authentication
import io.reactivex.Flowable
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.Executors

class IdentityTokenValidatorSpec extends Specification {

    private static final ValidateTokenResponse VALID_RESPONSE = new ValidateTokenResponse(ValidateTokenResponseSpec.USER_ID, ValidateTokenResponseSpec.VALID, ValidateTokenResponseSpec.VALID_CLAIMS)
    private static final ValidateTokenResponse INVALID_RESPONSE = new ValidateTokenResponse(ValidateTokenResponseSpec.USER_ID, ValidateTokenResponseSpec.INVALID, [])
    private static final String clientUid = ValidateTokenResponseSpec.USER_ID

    @Unroll
    def "Validate #description"() {
        given:
        TokenUser tokenUser = new TokenUser("name")
        def clientId = "someClientId"
        def clientSecret = "someClientSecret"

        tokenUser.clientId = clientUid

        and: "token validator"
        def identityTokenValidatorClient = Mock(IdentityTokenValidatorClient) {
            validateToken(_ as String, _ as ValidateTokenRequest, clientId + ":" + clientSecret) >> Flowable.just(identityResponse)
        }
        def tokenValidator = new IdentityTokenValidator(identityTokenValidatorClient, clientId, clientSecret, [tokenUser], Executors.newCachedThreadPool())

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
