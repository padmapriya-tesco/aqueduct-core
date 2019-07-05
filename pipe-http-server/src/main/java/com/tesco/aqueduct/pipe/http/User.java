package com.tesco.aqueduct.pipe.http;

import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UserDetails;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collections;
import java.util.List;

@EachProperty("authentication.users")
@ToString
@EqualsAndHashCode
class User {
    final String username;

    //TODO: we should switch to use salted hashes as soon as possible:
    String password;
    List<String> roles;

    User(@Parameter String username) {
        this.username = username;
    }

    boolean isAuthenticated(Object identity, Object secret) {
        return username.equals(identity) && password.equals(secret);
    }

    AuthenticationResponse toAuthenticationResponse() {
        return new UserDetails(username, roles != null ? roles : Collections.emptyList());
    }
}
