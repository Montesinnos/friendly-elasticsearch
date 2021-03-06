package com.montesinnos.friendly.elasticsearch.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FriendlyClientTest {

    @Test
    void generateIDsTest() {
        final String id = FriendlyClient.generateIDs();
        assertEquals(id, id.toLowerCase());
        assertEquals(20, id.length());
    }
}