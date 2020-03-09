package com.tesco.aqueduct.pipe.identity.issuer

import spock.lang.Specification

class IssueTokenResponseSpec extends Specification {

    def "token is expired when token expiry time has passed since it was created "() {
        given: "an identity token with expiry time"
        def identityToken = new IssueTokenResponse("someAccessToken", 1)

        when: "one second is passed"
        sleep(1000)

        then: "token is expired"
        identityToken.isTokenExpired()
    }

    def "token is valid when token expiry time has not expired yet"() {
        expect: "token is valid"
        !new IssueTokenResponse("someAccessToken", 1).isTokenExpired()
    }
}
