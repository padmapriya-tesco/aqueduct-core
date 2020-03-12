package com.tesco.aqueduct.pipe.identity.validator;

import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EachProperty("authentication.identity.users")
@ToString
@EqualsAndHashCode
public class TokenUser {
    final String name;
    String clientId;
    List<String> roles;

    TokenUser(@Parameter final String name) {
        this.name = name;
    }
}
