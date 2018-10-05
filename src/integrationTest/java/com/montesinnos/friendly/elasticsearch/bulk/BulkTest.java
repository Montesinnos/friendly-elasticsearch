package com.montesinnos.friendly.elasticsearch.bulk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.montesinnos.friendly.commons.resources.ResourceUtils;
import com.montesinnos.friendly.elasticsearch.Wrapper;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import com.montesinnos.friendly.elasticsearch.connection.Connection;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BulkTest {
    private final Connection connection = Wrapper.getConnection();

    @Test
    void indexTest() throws IOException {
        final Bulk bulk = new Bulk(connection.getClient());
        final FriendlyClient client = new FriendlyClient(connection.getClient());

        final String indexName = "test-bulk-insert";
        final String typeName = "pokemon";
        client.deleteIndex(indexName);
        client.createIndex(indexName, typeName, ResourceUtils.read("setup/pokemon-mapping.json"));

        new ObjectMapper()
                .readTree(ResourceUtils.read("setup/pokemon.json"))
                .forEach(x -> bulk.insert(indexName, typeName, x.toString()));

        bulk.close();
        client.flush(indexName);
        assertEquals(410L, client.count(indexName));
    }

    @Test
    void flushTest() {
        final Bulk bulk = new Bulk(connection.getClient());
        bulk.flush();

    }
}