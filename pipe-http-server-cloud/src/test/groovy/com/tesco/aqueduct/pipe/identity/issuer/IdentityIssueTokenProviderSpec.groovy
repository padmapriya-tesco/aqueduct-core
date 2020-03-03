package com.tesco.aqueduct.pipe.identity.issuer

import com.tesco.aqueduct.pipe.api.IdentityToken
import spock.lang.Specification

class IdentityIssueTokenProviderSpec extends Specification {

    public static final String IDENTITY_CLIENT_ID = "identityClientId"
    public static final String IDENTITY_CLIENT_SECRET = "identityClientSecret"

    def "token provider returns issued Identity token from Identity client" () {
        given: "a mock Identity client"
        IdentityIssueTokenClient identityIssueTokenClient = Mock()

        IdentityIssueTokenProvider identityIssueTokenProvider = new IdentityIssueTokenProvider(
            identityIssueTokenClient,
            IDENTITY_CLIENT_ID,
            IDENTITY_CLIENT_SECRET
        )

        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
                IDENTITY_CLIENT_ID,
                IDENTITY_CLIENT_SECRET
        )

        IssueTokenResponse issueTokenResponse = new IssueTokenResponse(
                "accessToken",
                10L
        )

        when: "A call is made to retrieve an Identity token"
        IdentityToken identityToken = identityIssueTokenProvider.retrieveIdentityToken()

        then: "issue token client is called, and token is provided"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> issueTokenResponse

        identityToken.accessToken == "accessToken"
        identityToken.tokenExpiry == 10L
    }


}
