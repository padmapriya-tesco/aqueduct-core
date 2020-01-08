package com.tesco.aqueduct.pipe.identity;

import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UserDetails;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

@EachProperty("authentication.identity.users")
@ToString
@EqualsAndHashCode
class TokenUser {
    final String clientId;
    List<String> roles;

    TokenUser(@Parameter final String clientId) {
        this.clientId = clientId;
    }

    AuthenticationResponse toAuthenticationResponse() {
        return new UserDetails(clientId, roles != null ? roles : Collections.emptyList());
    }
}
