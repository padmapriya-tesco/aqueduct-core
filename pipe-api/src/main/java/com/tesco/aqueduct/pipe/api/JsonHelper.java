package com.tesco.aqueduct.pipe.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper to use one way of parsing Message across the pipe projects
 */
public class JsonHelper {
    // it should not be public, is because of the way we limiting response size
    public static final ObjectMapper MAPPER = configureObjectMapper(new ObjectMapper());


    public static ObjectMapper configureObjectMapper(ObjectMapper mapper) {
        return mapper.registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module())
            .registerModule(new ParameterNamesModule())

            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

            .enable(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
            .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
            .enable(JsonParser.Feature.ALLOW_COMMENTS)

            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private static final CollectionType
        messageListType = MAPPER.getTypeFactory().constructCollectionType(List.class, Message.class);

    public static Message messageFromJson(String json) throws IOException {
        return MAPPER.readValue(json, Message.class);
    }

    public static List<Message> messageFromJsonArray(String json) throws IOException {
        return MAPPER.readValue(json, messageListType);
    }

    public static String toJson(Object msg) throws IOException {
        return MAPPER.writeValueAsString(msg);
    }

    public static String toJson(List<?> msg) throws IOException {
        return MAPPER.writeValueAsString(msg);
    }
}
