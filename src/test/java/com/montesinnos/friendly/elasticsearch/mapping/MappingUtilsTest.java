package com.montesinnos.friendly.elasticsearch.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MappingUtilsTest {

    @Test
    void hasIdTest() {
        final String json = "{\n" +
                "  \"doc\": {\n" +
                "    \"properties\": {\n" +
                "      \"id\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"phone\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"rating_value\": {\n" +
                "        \"type\": \"float\"\n" +
                "      }" +
                "}" +
                "}" +
                "}";

        assertTrue(MappingUtils.hasId(json));
    }
}