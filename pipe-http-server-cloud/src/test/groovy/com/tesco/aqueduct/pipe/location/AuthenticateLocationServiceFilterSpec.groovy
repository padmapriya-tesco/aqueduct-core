package com.tesco.aqueduct.pipe.location

import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.pipe.identity.CloudIdentityToken
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.filter.ClientFilterChain
import spock.lang.Specification

class AuthenticateLocationServiceFilterSpec extends Specification {

    def "Identity token provided by identity token provider is set in header as basic auth"() {
        given:
        def identityTokenProvider = Mock(TokenProvider)
        def httpClientFilter = new AuthenticateLocationServiceFilter(identityTokenProvider)

        and: "Mutable http request"
        def httpRequest = Mock(MutableHttpRequest)

        and: "filter chain to forward the request to"
        def filterChain = Mock(ClientFilterChain)

        when:
        httpClientFilter.doFilter(httpRequest, filterChain)

        then: "identity token is fetched"
        1 * identityTokenProvider.retrieveIdentityToken() >> new CloudIdentityToken("someAccessToken", 3599)

        and: "http header as basic auth containing the token is set within the mutable request"
        1 * httpRequest.bearerAuth("someAccessToken") >> httpRequest

        and: "the mutated http request with bearer auth is forwarded down the filter chain"
        1 * filterChain.proceed(httpRequest)
    }
}
