package com.montesinnos.friendly.elasticsearch.document;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class Document {
    private final RestHighLevelClient client;

    private final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public Document(final RestHighLevelClient client) {
        this.client = client;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public String insert(final String index, final String type, final String json) {
        final IndexRequest request = new IndexRequest(
                index,
                type);
        request.source(json, XContentType.JSON);
        return insert(request);
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
        return insert(request);
    }

    /**
     * Executes the index request
     *
     * @param request index request
     * @return id of document that was changed
     */
    public String insert(final IndexRequest request) {
        try {
            final IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            return indexResponse.getId();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String update(final String index, final String type, final String id, final Object object) {
        final UpdateRequest updateRequest = new UpdateRequest(index, type, id);

        try {
            updateRequest.doc(objectMapper.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        final UpdateResponse update;
        try {
            update = this.client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return update.getGetResult().sourceAsString();
    }


    public DocWriteResponse.Result delete(final String index, final String type, final String id) {
        final DeleteRequest request = new DeleteRequest(
                index,
                type,
                id);
        try {
            final DeleteResponse deleteResponse = client.delete(
                    request, RequestOptions.DEFAULT);

            final long version = deleteResponse.getVersion();
//
//            final ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
//            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
//
//            }
//            if (shardInfo.getFailed() > 0) {
//                for (ReplicationResponse.ShardInfo.Failure failure :
//                        shardInfo.getFailures()) {
//                    String reason = failure.reason();
//                }
//            }
            return deleteResponse.getResult();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}