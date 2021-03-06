package com.montesinnos.friendly.elasticsearch.document;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class ImmediateDocument implements Document {
    private final RestHighLevelClient client;
    private final DocumentClient documentClient;
    private final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    private final static String TYPE_NAME = "_doc"; //Delete this when we go to 7.0

    public ImmediateDocument(final RestHighLevelClient client) {
        this.client = client;
        this.documentClient = new DocumentClient(client);
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public String insert(final String index, final String json) {
        final IndexRequest request = new IndexRequest(
                index,
                TYPE_NAME);
        request.source(json, XContentType.JSON);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        return documentClient.insert(request);
    }

    public String insert(final String index, final String id, final Object object) {
        try {
            return insert(index, id, objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String insert(final String index, final String id, final String object) {
        final IndexRequest request = new IndexRequest(
                index,
                TYPE_NAME,
                id);
        request.source(object, XContentType.JSON);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        return documentClient.insert(request);
    }


    public String update(final String index, final String id, final Object object) {
        try {
            return update(index, id, objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public String update(final String index, final String id, final String json) {
        final UpdateRequest request = new UpdateRequest(index, TYPE_NAME, id);
        request.doc(json, XContentType.JSON);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        return documentClient.update(request);
    }

    public String updateAndGet(final String index, final String id, final Object object) {
        try {
            return update(index, id, objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public String updateAndGet(final String index, final String id, final String json) {
        final UpdateRequest request = new UpdateRequest(index, TYPE_NAME, id);
        request.doc(json, XContentType.JSON);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        return documentClient.updateAndGet(request);
    }


    public DocWriteResponse.Result delete(final String index, final String id) {
        final DeleteRequest request = new DeleteRequest(
                index,
                TYPE_NAME,
                id);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        return documentClient.delete(request);
    }
}