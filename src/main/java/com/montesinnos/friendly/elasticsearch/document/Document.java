package com.montesinnos.friendly.elasticsearch.document;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.client.RestHighLevelClient;

public interface Document {

    RestHighLevelClient getClient();

    String insert(final String index, final String json);

    String insert(final String index, final String id, final Object object);

    String insert(final String index, final String id, final String json);

    String update(final String index, final String id, final Object update);

    String update(final String index, final String id, final String json);

    String updateAndGet(final String index, final String id, final Object update);

    String updateAndGet(final String index, final String id, final String json);

    DocWriteResponse.Result delete(final String index, final String id);
}
