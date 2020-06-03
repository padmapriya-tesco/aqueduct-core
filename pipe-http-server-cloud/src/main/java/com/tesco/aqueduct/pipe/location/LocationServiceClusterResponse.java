package com.tesco.aqueduct.pipe.location;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationServiceClusterResponse {

    private final List<String> clusters;

    @JsonCreator
    public LocationServiceClusterResponse(@JsonProperty("clusters") List<String> clusters) {
        this.clusters = clusters;
    }

    public List<String> getClusters() {
        return clusters;
    }
}
