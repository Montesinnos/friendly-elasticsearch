package com.montesinnos.friendly.elasticsearch.document;

import org.elasticsearch.client.RestHighLevelClient;

public class DocumentFactory {
    public static Document immediate(final RestHighLevelClient client) {
        return new ImmediateDocument(client);
    }

    public static Document regular(final RestHighLevelClient client) {
        return new RegularDocument(client);
    }

    public static Document async(final RestHighLevelClient client) {
        throw new RuntimeException("Not ready!");
    }
}
