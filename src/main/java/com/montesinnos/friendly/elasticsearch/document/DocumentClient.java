package com.montesinnos.friendly.elasticsearch.document;

import com.fasterxml.jackson.annotation.JsonInclude;
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
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;

public class DocumentClient {
    private final RestHighLevelClient client;
    private final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public DocumentClient(RestHighLevelClient client) {
        this.client = client;
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


    public String update(final UpdateRequest updateRequest) {
        final UpdateResponse updateResponse;
        try {
            updateResponse = this.client.update(updateRequest, RequestOptions.DEFAULT);
            long version = updateResponse.getVersion();
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                return "CREATED";
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                return "UPDATED";
            } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                return "DELETED";
            } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                return "NOOP";
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return updateResponse.getGetResult().sourceAsString();
    }

    public String updateAndGet(final UpdateRequest updateRequest) {
        updateRequest.fetchSource(true);
        final UpdateResponse updateResponse;
        try {
            updateResponse = this.client.update(updateRequest, RequestOptions.DEFAULT);
            final GetResult result = updateResponse.getGetResult();
            if (result.isExists()) {
                return result.sourceAsString();
            } else {
                throw new RuntimeException("Couldn't fetch the source after update");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public DocWriteResponse.Result delete(final DeleteRequest deleteRequest) {
        try {
            final DeleteResponse deleteResponse = client.delete(
                    deleteRequest, RequestOptions.DEFAULT);

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
