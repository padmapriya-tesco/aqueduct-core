package com.tesco.aqueduct.pipe.identity.issuer

import com.tesco.aqueduct.pipe.api.IdentityToken
import spock.lang.Specification

class IdentityIssueTokenProviderSpec extends Specification {

    public static final String IDENTITY_CLIENT_ID = "identityClientId"
    public static final String IDENTITY_CLIENT_SECRET = "identityClientSecret"

    private IdentityIssueTokenClient identityIssueTokenClient
    private IdentityIssueTokenProvider identityIssueTokenProvider

    void setup() {
        identityIssueTokenClient = Mock()

        identityIssueTokenProvider = new IdentityIssueTokenProvider(
                identityIssueTokenClient,
                IDENTITY_CLIENT_ID,
                IDENTITY_CLIENT_SECRET
        )
    }

    def "token provider returns issued Identity token from Identity client" () {
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
                IDENTITY_CLIENT_ID,
                IDENTITY_CLIENT_SECRET
        )

        and:"an issue token response"
        IssueTokenResponse issueTokenResponse = new IssueTokenResponse("accessToken", 10)

        when: "A call is made to retrieve an Identity token"
        IdentityToken identityToken = identityIssueTokenProvider.retrieveIdentityToken()

        then: "issue token client is called, and token is provided"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> issueTokenResponse

        and: "token is as expected"
        identityToken.accessToken == "accessToken"
        !identityToken.isTokenExpired()
    }

    def "token is cached if it is not expired" () {
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
                IDENTITY_CLIENT_ID,
                IDENTITY_CLIENT_SECRET
        )

        and: "an issue token response"
        IssueTokenResponse issueTokenResponse = new IssueTokenResponse("accessToken", 10)

        when: "a call is made to retrieve an Identity token"
        IdentityToken firstIdentityToken = identityIssueTokenProvider.retrieveIdentityToken()

        then: "issue token client is called, and token is provided"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> issueTokenResponse

        and: "token is as expected"
        firstIdentityToken.accessToken == "accessToken"
        !firstIdentityToken.isTokenExpired()

        when: "we fetch the token again"
        IdentityToken secondIdentityToken = identityIssueTokenProvider.retrieveIdentityToken()

        then: "issue token client is not called again"
        0 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest)

        and: "previously fetched token is provided again"
        secondIdentityToken == firstIdentityToken
    }

    def "token is issued again if it has expired"(){
        given: "an issue token request"
        IssueTokenRequest issueTokenRequest = new IssueTokenRequest(
                IDENTITY_CLIENT_ID,
                IDENTITY_CLIENT_SECRET
        )

        and: "an issue token response"
        IssueTokenResponse issueTokenResponse = new IssueTokenResponse("accessToken", 0)

        when: "a call is made to retrieve an Identity token"
        IdentityToken firstIdentityToken = identityIssueTokenProvider.retrieveIdentityToken()

        then: "issue token client is called, and token is provided"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest) >> issueTokenResponse

        and: "token is expired"
        firstIdentityToken.accessToken == "accessToken"
        firstIdentityToken.isTokenExpired()

        when: "we fetch the token again"
        identityIssueTokenProvider.retrieveIdentityToken()

        then: "a new token is issued"
        1 * identityIssueTokenClient.retrieveIdentityToken(_ as String, issueTokenRequest)
    }
}
