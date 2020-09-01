package com.tesco.aqueduct.registry.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tesco.aqueduct.pipe.api.JsonHelper;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;

import java.util.List;

@Singleton
public class NodeFactory {

    @Value("${registry.target.version:0.0.0}")
    private String targetVersion;

    public List<Node> getNodesFromJson(final String jsonEntry) throws JsonProcessingException {
        List<Node> nodeList = fromJson(jsonEntry);
        nodeList.forEach(n -> n.setSemanticTargetVersion(targetVersion));
        return nodeList;
    }

    private List<Node> fromJson(final String jsonEntry) throws JsonProcessingException {
        ObjectMapper jsonMapper = JsonHelper.MAPPER;
        final JavaType type = jsonMapper.getTypeFactory().constructCollectionType(List.class, Node.class);
        return jsonMapper.readValue(jsonEntry, type);
    }
}
