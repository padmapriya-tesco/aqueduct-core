package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Cluster {
    private final String id;
    @JsonCreator
    public Cluster(@JsonProperty("id") String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
