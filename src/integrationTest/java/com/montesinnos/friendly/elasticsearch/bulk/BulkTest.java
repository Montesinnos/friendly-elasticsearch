package com.montesinnos.friendly.elasticsearch.bulk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.montesinnos.friendly.elasticsearch.Wrapper;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import com.montesinnos.friendly.elasticsearch.connection.Connection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BulkTest {
    private final Connection connection = Wrapper.getConnection();

    @Test
    void indexTest() throws IOException {
        final Bulk bulk = new Bulk(connection.getClient());
        final FriendlyClient client = new FriendlyClient(connection.getClient());

        final URL url = Resources.getResource("setup/pokemon.json");
        final URL mapping = Resources.getResource("setup/pokemon-mapping.json");

        final String indexName = "test-bulk-insert";
        final String typeName = "pokemon";
        client.deleteIndex(indexName);
        client.createIndex(indexName, typeName, Resources.toString(mapping, Charsets.UTF_8));

        new ObjectMapper()
                .readTree(Resources.toString(url, Charsets.UTF_8))
                .forEach(x -> bulk.insert(indexName, "pokemon", x.toString()));

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