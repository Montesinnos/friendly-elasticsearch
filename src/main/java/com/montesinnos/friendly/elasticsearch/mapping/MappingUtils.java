package com.montesinnos.friendly.elasticsearch.mapping;


import com.montesinnos.friendly.commons.json.JsonUtils;

import java.io.IOException;

public class MappingUtils {

    public static boolean hasId(final String mapping) {
        try {
            JsonUtils.objectMapper.readTree(mapping)
                    .get("doc")
                    .get("properties")
                    .get("id")
                    .asText();
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
