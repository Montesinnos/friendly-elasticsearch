package com.montesinnos.friendly.elasticsearch.client;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.montesinnos.friendly.elasticsearch.Wrapper;
import com.montesinnos.friendly.elasticsearch.connection.Connection;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FriendlyClientTest {
    private static final String createIndexName = "test-delete-me-" + FriendlyClient.generateIDs().toLowerCase();
    private static final String createIndexFromSettingsName = "test-delete-me-" + FriendlyClient.generateIDs().toLowerCase();
    private static final String deleteIndexName = FriendlyClient.generateIDs();
    private static Connection connection;

    @BeforeAll
    static void setup() {
        connection = Wrapper.setup();
    }

    @AfterAll
    static void tearDown() {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        client.deleteIndex(createIndexName);
        client.deleteIndex(deleteIndexName);
    }

    @Test
    @DisplayName("Wait for green")
    void waitForGreenTest() {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        assertEquals(ClusterHealthStatus.GREEN, client.waitForGreen(10));
    }

    @Test
    @DisplayName("Index count")
    void countTest() {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        assertEquals(Wrapper.INDEX_COUNT, client.count(Wrapper.INDEX_NAME));
    }

    @Test
    @DisplayName("Index flush")
    void flushTest() {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        assertTrue(client.flush(Wrapper.INDEX_NAME) > 0);
    }

    @Test
    @DisplayName("Create Index")
    void createIndexTest() {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        client.deleteIndex(createIndexName);
        assertEquals(createIndexName, client.createIndex(createIndexName));
    }

    @Test
    void createIndexFromSettingsTest() throws IOException {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        final String settings = Resources.toString(
                Resources.getResource("client/pokemon-settings.json"), Charsets.UTF_8);
        client.deleteIndex(createIndexFromSettingsName);
        assertEquals(createIndexFromSettingsName, client.createIndexFromSettings(createIndexFromSettingsName, settings));
    }

    @Test
    @DisplayName("Index exists")
    void indexExistsTest() {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        assertFalse(client.indexExists("garbage-don-t-work"));
        assertTrue(client.indexExists(Wrapper.INDEX_NAME));
    }

    @Test
    @DisplayName("Delete index")
    void deleteIndexTest() {
        final FriendlyClient client = new FriendlyClient(connection.getClient());
        client.deleteIndex(deleteIndexName);
        client.createIndex(deleteIndexName);
        assertTrue(client.deleteIndex(deleteIndexName));
    }
}