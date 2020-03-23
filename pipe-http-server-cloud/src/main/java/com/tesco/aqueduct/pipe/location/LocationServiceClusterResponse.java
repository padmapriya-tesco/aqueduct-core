package com.tesco.aqueduct.pipe.location;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationServiceClusterResponse {

    private final List<Cluster> clusters;

    @JsonCreator
    public LocationServiceClusterResponse(@JsonProperty("clusters") List<Cluster> clusters) {
        this.clusters = clusters;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Cluster {
        private final String id;
        @JsonCreator
        private Cluster(@JsonProperty("id") String id) {
            this.id = id;
        }
        public String getId() {
            return id;
        }
    }
}
