package com.montesinnos.friendly.elasticsearch.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.SyncedFlushResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

public class FriendlyClient {
    private static final Logger logger = LogManager.getLogger(FriendlyClient.class);

    private final RestHighLevelClient client;
    private final RestClient lowLevelClient;

    public FriendlyClient(final RestHighLevelClient client) {
        this.client = client;
        lowLevelClient = client.getLowLevelClient();
    }

    public static String generateIDs() {
        return UUIDs.base64UUID();
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public ClusterHealthStatus waitForGreen(final int seconds) {
        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForStatus(ClusterHealthStatus.GREEN);
        request.timeout(TimeValue.timeValueSeconds(seconds));

        final ClusterHealthResponse response;
        try {
            response = client.cluster().health(request, RequestOptions.DEFAULT);
            return response.getStatus();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ClusterHealthStatus.RED;
    }

    public long count(final String index) {
        final SearchRequest searchRequest = new SearchRequest(index);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(0);
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);

        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse.getHits().totalHits;
        } catch (ElasticsearchStatusException e) {
//            e.printStackTrace();
        } catch (IOException e) {
//            e.printStackTrace();
        }

//        TODO wait until they release this
//        CountRequest countRequest = new CountRequest("index");
        return 0L;
    }

    /**
     * Creates an index using a full JSON of its settings, mappings, and alias
     *
     * @param index    name of the index to be created
     * @param settings JSON String of the entire settings for the index. You can see
     *                 how it looks like by checking the settings of existings indices
     *                 on Elasticsearch
     * @return name of the index created
     */
    public String createIndexFromSettings(final String index, final String settings) {
        final CreateIndexRequest request = new CreateIndexRequest(index);
        request.source(settings, XContentType.JSON);
        return createIndex(request);
    }

    /**
     * Executes the CreateIndex request
     *
     * @param request to be executed
     * @return name of the index created
     */
    public String createIndex(final CreateIndexRequest request) {
        final CreateIndexResponse createIndexResponse;
        try {
            createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            return createIndexResponse.index();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Creates an index using the name provided
     * It's a no-op if the index already exists
     *
     * @param index name for the index
     * @return name of the index created
     */
    public String createIndex(final String index) {
        return createIndex(index, null, null);
    }

    /**
     * Creates an index using the name provided, generating a type with the mapping
     * It's a no-op if the index already exists
     *
     * @param index   name for the index
     * @param type    name for the type
     * @param mapping Json string containing the mapping definition
     * @return name of the index created
     */
    public String createIndex(final String index, final String type, final String mapping) {
        final CreateIndexRequest request = new CreateIndexRequest(index);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", -1)
        );
        request.timeout(TimeValue.timeValueMinutes(2));
        if (Strings.isNotBlank(mapping)) {
            request.mapping(type,
                    mapping,
                    XContentType.JSON);
        }

        return createIndex(request);
    }

    /**
     * Creates an index using the name provided, generating a type with the mapping
     *
     * @param index   name for the index
     * @param type    name for the type
     * @param mapping Json string containing the mapping definition
     * @param force   forces the creation of the index, by deleting any existing index of the same name
     * @return name of the name created
     */
    public String createIndex(final String index, final String type, final String mapping, final boolean force) {
        if (force) {
            deleteIndex(index);
        }
        return createIndex(index, type, mapping);
    }

    public boolean deleteIndex(final String index) {
        if (!indexExists(index)) {
            return true;
        }
        final DeleteIndexRequest request = new DeleteIndexRequest(index);
        request.timeout(TimeValue.timeValueMinutes(2));
        request.indices(index);
        try {
            final AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            return deleteIndexResponse.isAcknowledged();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Checks if the index provided exists
     *
     * @param index index name to be checked
     * @return true if index exists
     */
    public boolean indexExists(final String index) {
        final GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        try {
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Issues a flush command to Elastic
     *
     * @param index Index name to be flushed. Blank or all of them
     * @return the percent of successful shards
     */
    public int flush(final String index) {
        final FlushRequest request =
                Strings.isNotBlank(index) ?
                        new FlushRequest(index.trim()) :
                        new FlushRequest();

        request.waitIfOngoing(true);
        request.force(true);

        try {
            final FlushResponse flushResponse = client.indices().flush(request, RequestOptions.DEFAULT);

            final int totalShards = flushResponse.getTotalShards();
            final int successfulShards = flushResponse.getSuccessfulShards();
//            final int failedShards = flushResponse.getFailedShards();
//            final DefaultShardOperationFailedException[] failures = flushResponse.getShardFailures();

            return successfulShards * 100 / totalShards;
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                logger.warn("[{}] index not found to be flushed", index);
            }
        } catch (IOException e) {
            e.printStackTrace();
//            throw new RuntimeException();
        }
        return 0;
    }

    public int flush() {
        return flush("");
    }

    /**
     * Issues a flush (sync) command to Elastic
     *
     * @param index Index name to be flushed. Blank or all of them
     * @return the percent of successful shards
     */
    public int flushSync(final String index) {
        final SyncedFlushRequest request =
                Strings.isNotBlank(index) ?
                        new SyncedFlushRequest(index.trim()) :
                        new SyncedFlushRequest();
        try {
            final SyncedFlushResponse flushSyncedResponse = client.indices().flushSynced(request, RequestOptions.DEFAULT);

            final int totalShards = flushSyncedResponse.totalShards();
            final int successfulShards = flushSyncedResponse.successfulShards();
            final int failedShards = flushSyncedResponse.failedShards();

            for (Map.Entry<String, SyncedFlushResponse.IndexResult> responsePerIndexEntry :
                    flushSyncedResponse.getIndexResults().entrySet()) {
                final String indexName = responsePerIndexEntry.getKey();
                final SyncedFlushResponse.IndexResult indexResult = responsePerIndexEntry.getValue();
                int totalShardsForIndex = indexResult.totalShards();
                int successfulShardsForIndex = indexResult.successfulShards();
                int failedShardsForIndex = indexResult.failedShards();
                if (failedShardsForIndex > 0) {
                    for (SyncedFlushResponse.ShardFailure failureEntry : indexResult.failures()) {
                        int shardId = failureEntry.getShardId();
                        String failureReason = failureEntry.getFailureReason();
                        Map<String, Object> routing = failureEntry.getRouting();
                    }
                }
            }
            return successfulShards * 100 / totalShards;
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                logger.warn("[{}] index not found to be flushed", index);
            }
        } catch (IOException e) {
            e.printStackTrace();
//            throw new RuntimeException();
        }
        return 0;
    }

    public int flushSync() {
        return flushSync("");
    }

    /**
     * Refreshes an index. Use it after inserting records
     *
     * @param index index name to be refreshed
     * @return number of shards affected
     */
    public int refresh(final String index) {
        final RefreshRequest request =
                Strings.isNotBlank(index) ?
                        new RefreshRequest(index.trim()) :
                        new RefreshRequest();

        try {
            final RefreshResponse refreshResponse = client.indices().refresh(request, RequestOptions.DEFAULT);
            return refreshResponse.getTotalShards();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Refreshes all indexes
     *
     * @return number of shards affected
     */
    public int refresh() {
        return refresh("");
    }

    public void close() throws RuntimeException {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
