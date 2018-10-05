package com.montesinnos.friendly.elasticsearch.connection;

import org.junit.jupiter.api.Test;

public class ConnectionTest {
    @Test
    public void testConnection() {
        final Connection connection = new Connection("localhost", 59200, "http");
    }
}