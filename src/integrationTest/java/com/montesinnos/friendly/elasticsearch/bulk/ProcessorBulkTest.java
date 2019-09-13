package com.montesinnos.friendly.elasticsearch.bulk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.montesinnos.friendly.commons.resources.ResourceUtils;
import com.montesinnos.friendly.elasticsearch.Wrapper;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import com.montesinnos.friendly.elasticsearch.connection.Connection;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProcessorBulkTest {
    private final Connection connection = Wrapper.getConnection();

    @Test
    void indexTest() throws IOException {
        final String indexName = "test-processor-bulk-insert";
        final String typeName = "pokemon";

        final Bulk bulk = new ProcessorBulk(connection, new BulkConfiguration.Builder()
                .indexName(indexName)
                .build());
        final FriendlyClient client = new FriendlyClient(connection);

        client.deleteIndex(indexName);
        client.createIndex(indexName, ResourceUtils.read("setup/pokemon-mapping.json"));

        new ObjectMapper()
                .readTree(ResourceUtils.read("setup/pokemon.json"))
                .forEach(line -> bulk.insert(line.toString()));

        bulk.close();
        client.refresh(indexName);
        client.flush(indexName);
        assertEquals(410L, client.count(indexName));
        client.deleteIndex(indexName);
    }

    @Test
    void flushTest() {
        final ProcessorBulk processorBulk = new ProcessorBulk(connection);
        processorBulk.flush();
    }
}