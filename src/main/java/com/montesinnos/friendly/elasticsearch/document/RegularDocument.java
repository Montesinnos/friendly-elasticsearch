package com.montesinnos.friendly.elasticsearch.document;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class RegularDocument implements Document {
    private final RestHighLevelClient client;
    private final DocumentClient documentClient;
    private final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public RegularDocument(final RestHighLevelClient client) {
        this.client = client;
        this.documentClient = new DocumentClient(client);
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public String insert(final String index, final String type, final String json) {
        final IndexRequest request = new IndexRequest(
                index,
                type);
        request.source(json, XContentType.JSON);
        return documentClient.insert(request);
    }

    public String insert(final String index, final String type, final String id, final Object object) {
        try {
            return insert(index, type, id, objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String insert(final String index, final String type, final String id, final String object) {
        final IndexRequest request = new IndexRequest(
                index,
                type,
                id);
        request.source(object, XContentType.JSON);
        return documentClient.insert(request);
    }

    public String update(final String index, final String type, final String id, final Object object) {
        try {
            return update(index, type, id, objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public String update(final String index, final String type, final String id, final String json) {
        final UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.doc(json, XContentType.JSON);
        return documentClient.update(updateRequest);
    }

    public String updateAndGet(final String index, final String type, final String id, final Object object) {
        try {
            return update(index, type, id, objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public String updateAndGet(final String index, final String type, final String id, final String json) {
        final UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.doc(json, XContentType.JSON);
        return documentClient.updateAndGet(updateRequest);
    }

    public DocWriteResponse.Result delete(final String index, final String type, final String id) {
        final DeleteRequest deleteRequest = new DeleteRequest(
                index,
                type,
                id);

        return documentClient.delete(deleteRequest);
    }
}