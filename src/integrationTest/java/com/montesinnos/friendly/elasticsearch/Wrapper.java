package com.montesinnos.friendly.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.montesinnos.friendly.commons.resources.ResourceUtils;
import com.montesinnos.friendly.elasticsearch.bulk.Bulk;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import com.montesinnos.friendly.elasticsearch.connection.Connection;

import java.io.IOException;

public class Wrapper {
    private static final String host = "localhost";
    private static final int port = 59200;
    private static final String protocol = "http";
    public static String INDEX_NAME = "test-pokemons";
    public static String TYPE_NAME = "pokemon";
    public static long INDEX_COUNT = 410L;

    public static Connection getConnection() {
        return new Connection(host, port, protocol);
    }

    public static Connection setup() {
        final Connection connection = getConnection();
        final Bulk bulk = new Bulk(connection.getClient());
        final FriendlyClient client = new FriendlyClient(connection.getClient());

        client.deleteIndex(INDEX_NAME);
            client.createIndex(INDEX_NAME, TYPE_NAME, ResourceUtils.read("setup/pokemon-mapping.json"));

        try {
            new ObjectMapper()
                    .readTree(ResourceUtils.read("setup/pokemon.json"))
                    .forEach(x -> bulk.insert(INDEX_NAME, TYPE_NAME, x.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        bulk.close();
        client.flush(INDEX_NAME);
        return connection;
    }
}
