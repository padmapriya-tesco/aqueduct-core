package com.tesco.aqueduct.pipe.location

import com.tesco.aqueduct.pipe.api.IdentityToken
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenProvider
import spock.lang.Ignore
import spock.lang.Specification

@Ignore
class CloudLocationResolverSpec extends Specification {

    def "Identity token is issued and return empty list"() {
        given:
        def tokenProvider = Mock(IdentityIssueTokenProvider)

        when:
        def locations = new CloudLocationResolver(tokenProvider).resolve("someLocation")

        then:
        1 * tokenProvider.retrieveIdentityToken() >> Mock(IdentityToken)

        and: "locations are returned"
        locations == []
    }
}
