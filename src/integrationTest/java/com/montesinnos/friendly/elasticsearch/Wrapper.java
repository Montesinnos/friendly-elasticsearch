package com.montesinnos.friendly.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.montesinnos.friendly.elasticsearch.bulk.Bulk;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import com.montesinnos.friendly.elasticsearch.connection.Connection;

import java.io.IOException;
import java.net.URL;

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

        final URL url = Resources.getResource("setup/pokemon.json");
        final URL mapping = Resources.getResource("setup/pokemon-mapping.json");

        client.deleteIndex(INDEX_NAME);
        try {
            client.createIndex(INDEX_NAME, TYPE_NAME, Resources.toString(mapping, Charsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            new ObjectMapper()
                    .readTree(Resources.toString(url, Charsets.UTF_8))
                    .forEach(x -> bulk.insert(INDEX_NAME, TYPE_NAME, x.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        bulk.close();
        client.flush(INDEX_NAME);
        return connection;
    }
}
