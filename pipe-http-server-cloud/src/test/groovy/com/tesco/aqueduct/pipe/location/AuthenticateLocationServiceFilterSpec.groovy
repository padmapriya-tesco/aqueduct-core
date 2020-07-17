package com.tesco.aqueduct.pipe.location

import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.pipe.identity.issuer.IssueTokenResponse
import io.micronaut.http.HttpResponse
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.filter.ClientFilterChain
import io.reactivex.Single
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
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
        httpClientFilter.doFilter(httpRequest, filterChain).subscribe(withDoNothingSubscriber())

        then: "identity token is fetched"
        1 * identityTokenProvider.retrieveIdentityToken() >> Single.just(new IssueTokenResponse("someAccessToken", 3599))

        and: "http header as basic auth containing the token is set within the mutable request"
        1 * httpRequest.bearerAuth("someAccessToken") >> httpRequest

        and: "the mutated http request with bearer auth is forwarded down the filter chain"
        1 * filterChain.proceed(httpRequest)
    }

    private Subscriber<HttpResponse<?>> withDoNothingSubscriber() {
        new Subscriber<HttpResponse<?>>() {
            @Override
            void onSubscribe(Subscription s) {
                //do nothing
            }

            @Override
            void onNext(HttpResponse<?> httpResponse) {
                //do nothing
            }

            @Override
            void onError(Throwable t) {
                //do nothing

            }

            @Override
            void onComplete() {
                //do nothing
            }
        }
    }
}
