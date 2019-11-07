package com.tesco.aqueduct.pipe.http

import io.micronaut.security.authentication.Authentication
import io.reactivex.Flowable
import spock.lang.Specification
import spock.lang.Unroll

import static com.tesco.aqueduct.pipe.http.ValidateTokenResponseSpec.*

class IdentityTokenValidatorSpec extends Specification {

    private static final ValidateTokenResponse VALID_RESPONSE = new ValidateTokenResponse(USER_ID, VALID, VALID_CLAIMS)
    private static final ValidateTokenResponse INVALID_RESPONSE = new ValidateTokenResponse(USER_ID, INVALID, [])

    @Unroll
    def "Validate #description"() {
        given:
        def tokenValidator = new IdentityTokenValidator()
        def identityTokenValidatorClient = Mock(IdentityTokenValidatorClient) {
            validateToken(_ as String, _ as ValidateTokenRequest) >> Flowable.just(identityResponse)
        }

        tokenValidator.identityTokenValidatorClient = identityTokenValidatorClient

        when:
        def result = tokenValidator.validateToken("token") as Flowable

        then:
        predicate(result)

        where:
        identityResponse || predicate                                                           | description
        INVALID_RESPONSE || { Flowable r -> r.isEmpty().blockingGet() }                         | 'no authorisation if invalid identity response'
        VALID_RESPONSE   || { Flowable<Authentication> r -> r.blockingFirst().name == USER_ID } | 'authorised if valid identity response'
    }
}
