package com.montesinnos.friendly.elasticsearch.client;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.montesinnos.friendly.commons.resources.ResourceUtils;
import com.montesinnos.friendly.elasticsearch.Wrapper;
import com.montesinnos.friendly.elasticsearch.connection.Connection;
import com.montesinnos.friendly.elasticsearch.index.IndexConfiguration;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FriendlyClientTest {
    private static final String createIndexName = "test-delete-me-" + FriendlyClient.generateIDs();
    private static final String createIndexFromSettingsName = "test-delete-me-" + FriendlyClient.generateIDs();
    private static final String aliasName = "test-alias-" + FriendlyClient.generateIDs();
    private static final String deleteIndexName = FriendlyClient.generateIDs();
    private static Connection connection;

    @BeforeAll
    static void setup() {
        connection = Wrapper.setup();
    }

    @AfterAll
    static void tearDown() {
        final FriendlyClient client = new FriendlyClient(connection);
        client.deleteIndex(createIndexName);
        client.deleteIndex(deleteIndexName);
    }

    @Test
    @DisplayName("Wait for green")
    void waitForGreenTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        final long start = System.currentTimeMillis();
        final ClusterHealthStatus result = client.waitForGreen(30);
        final long end = System.currentTimeMillis();

        //Checks if the Cluster is green or if it actually waited that long
        assertTrue(ClusterHealthStatus.GREEN == result
                || (end - start) > 29000);
    }

    @Test
    @DisplayName("Index count")
    void countTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        assertEquals(Wrapper.INDEX_COUNT, client.count(Wrapper.INDEX_NAME));
    }

    @Test
    @DisplayName("Index flush")
    void flushTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        assertTrue(client.flush(Wrapper.INDEX_NAME) > 0);
    }

    @Test
    @DisplayName("Create Index")
    void createIndexTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        client.deleteIndex(createIndexName);
        assertEquals(createIndexName, client.createIndex(createIndexName));
    }

    @Test
    void createIndexFromSettingsTest() throws IOException {
        final FriendlyClient client = new FriendlyClient(connection);
        final String settings = Resources.toString(
                Resources.getResource("client/pokemon-settings.json"), Charsets.UTF_8);
        client.deleteIndex(createIndexFromSettingsName);
        assertEquals(createIndexFromSettingsName, client.createIndexFromSettings(createIndexFromSettingsName, settings));
    }

    @Test
    void createIndexFromConfiguratorTest() {
        final FriendlyClient client = new FriendlyClient(connection);

        final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .name(createIndexName)
                .mapping(ResourceUtils.read("setup/pokemon-mapping.json"))
                .sortField("animationTime")
                .sortOrder("desc")
                .numberOfShards(10)
                .numberOfReplicas(5)
                .refreshInterval(-1)
                .build();
        client.deleteIndex(createIndexName);
        assertEquals(createIndexName, client.createIndex(indexConfiguration));
    }

    @Test
    @DisplayName("Index exists")
    void indexExistsTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        assertFalse(client.indexExists("garbage-don-t-work"));
        assertTrue(client.indexExists(Wrapper.INDEX_NAME));
    }

    @Test
    @DisplayName("Delete index")
    void deleteIndexTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        client.deleteIndex(deleteIndexName);
        client.createIndex(deleteIndexName);
        assertTrue(client.deleteIndex(deleteIndexName));
    }

    @Test
    @DisplayName("Add and Remove Alias")
    void addAliasTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        client.deleteIndex(createIndexName);
        client.createIndex(createIndexName);
        final String alias = client.addAlias(createIndexName, aliasName);
        assertIterableEquals(new ArrayList<>(Collections.singletonList(alias)),
                client.getAliases(createIndexName));
    }

    @Test
    @DisplayName("Remove Alias")
    void removeAliasTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        client.deleteIndex(createIndexName);
        client.createIndex(createIndexName);
        final String alias = client.addAlias(createIndexName, aliasName);
        assertIterableEquals(new ArrayList<>(Collections.singletonList(alias)),
                client.getAliases(createIndexName));

        client.removeAlias(createIndexName, aliasName);
        assertTrue(client.getAliases(createIndexName).isEmpty());
    }

    @Test
    @DisplayName("Get Aliases")
    void getAliasesTest() {
        final FriendlyClient client = new FriendlyClient(connection);
        client.deleteIndex(createIndexName);
        client.createIndex(createIndexName);
        client.addAlias(createIndexName, aliasName);
        final List<String> aliases = client.getAliases(createIndexName);
        assertIterableEquals(new ArrayList<>(Collections.singletonList(aliasName)), aliases);
    }
}