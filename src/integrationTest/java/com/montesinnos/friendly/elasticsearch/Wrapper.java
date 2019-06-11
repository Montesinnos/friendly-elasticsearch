package com.montesinnos.friendly.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.montesinnos.friendly.commons.resources.ResourceUtils;
import com.montesinnos.friendly.elasticsearch.bulk.BulkConfiguration;
import com.montesinnos.friendly.elasticsearch.bulk.InMemoryBulk;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import com.montesinnos.friendly.elasticsearch.connection.Connection;

import java.io.IOException;

public class Wrapper {
    private static final String host = "localhost";
    private static final int port = 59200;
    private static final String protocol = "http";
    public static String INDEX_NAME = "test-pokemons";
    public static long INDEX_COUNT = 410L;

    public static Connection getConnection() {
        return new Connection(host, port, protocol);
    }

    public static Connection setup() {
        final Connection connection = getConnection();
        final InMemoryBulk bulk = new InMemoryBulk(connection,
                new BulkConfiguration.Builder()
                        .indexName(INDEX_NAME)
                        .build());
        final FriendlyClient client = new FriendlyClient(connection);

        client.deleteIndex(INDEX_NAME);
        client.createIndex(INDEX_NAME, ResourceUtils.read("setup/pokemon-mapping.json"));

        try {
            new ObjectMapper()
                    .readTree(ResourceUtils.read("setup/pokemon.json"))
                    .forEach(line -> bulk.insert(line.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        bulk.close();
        client.flush(INDEX_NAME);
        return connection;
    }
}
