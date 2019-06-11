package com.montesinnos.friendly.elasticsearch.client;

import com.montesinnos.friendly.elasticsearch.connection.Connection;
import com.montesinnos.friendly.elasticsearch.index.IndexConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.SyncedFlushResponse;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FriendlyClient {
    private static final Logger logger = LogManager.getLogger(FriendlyClient.class);

    private final Connection connection;

    public FriendlyClient(final Connection connection) {
        this.connection = connection;
    }

    public static String generateIDs() {
        return UUIDs.base64UUID().toLowerCase();
    }

    public RestHighLevelClient getClient() {
        return connection.getClient();
    }

    public ClusterHealthStatus waitForGreen(final int seconds) {
        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForStatus(ClusterHealthStatus.GREEN);
        request.timeout(TimeValue.timeValueSeconds(seconds));

        final ClusterHealthResponse response;
        try {
            response = getClient().cluster().health(request, RequestOptions.DEFAULT);
            return response.getStatus();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ClusterHealthStatus.RED;
    }

    public long count(final String index) {
        final CountRequest countRequest = new CountRequest(index);
        try {
            final CountResponse countResponse = getClient()
                    .count(countRequest, RequestOptions.DEFAULT);

            final long count = countResponse.getCount();
//            RestStatus status = countResponse.status();
//            Boolean terminatedEarly = countResponse.isTerminatedEarly();

            return count;
        } catch (ElasticsearchStatusException e) {
//            e.printStackTrace();
        } catch (IllegalStateException e) {
            logger.error("Couldn't count [{}]. Message: {}", index, e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

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
            createIndexResponse = getClient().indices().create(request, RequestOptions.DEFAULT);
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
        final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .name(index)
                .build();
        return createIndex(indexConfiguration);
    }

    /**
     * Creates an index using the name provided, generating a type with the mapping
     * It's a no-op if the index already exists
     *
     * @param index   name for the index
     * @param mapping Json string containing the mapping definition
     * @return name of the index created
     */
    public String createIndex(final String index, final String mapping) {

        final IndexConfiguration indexConfiguration = new IndexConfiguration.Builder()
                .name(index)
                .mapping(mapping).build();

        return createIndex(indexConfiguration);
    }

    public String createIndex(final IndexConfiguration indexConfiguration) {
        final CreateIndexRequest request = new CreateIndexRequest(indexConfiguration.getName());
        Settings.Builder builder = Settings.builder()
                .put("index.number_of_shards", indexConfiguration.getNumberOfShards())
                .put("index.number_of_replicas", indexConfiguration.getNumberOfReplicas())
                .put("index.refresh_interval", indexConfiguration.getRefreshInterval());

        request.setTimeout(TimeValue.timeValueMinutes(2));

        if (Strings.isNotBlank(indexConfiguration.getMapping())) {
            request.mapping(
                    indexConfiguration.getMapping(),
                    XContentType.JSON);
        }


        if (Strings.isNotBlank(indexConfiguration.getSortField())) {
            builder.put("index.sort.field", indexConfiguration.getSortField());
            builder.put("index.sort.order", indexConfiguration.getSortOrder());
        }
        request.settings(builder.build());
        return createIndex(request);
    }


    /**
     * Creates an index using the name provided, generating a type with the mapping
     *
     * @param index   name for the index
     * @param mapping Json string containing the mapping definition
     * @param force   forces the creation of the index, by deleting any existing index of the same name
     * @return name of the name created
     */
    public String createIndex(final String index, final String mapping, final boolean force) {
        if (force) {
            deleteIndex(index);
        }
        return createIndex(index, mapping);
    }

    public boolean deleteIndex(final String index) {
        if (!indexExists(index)) {
            return true;
        }
        final DeleteIndexRequest request = new DeleteIndexRequest(index);
        request.timeout(TimeValue.timeValueMinutes(2));
        request.indices(index);
        try {
            final AcknowledgedResponse deleteIndexResponse = getClient().indices().delete(request, RequestOptions.DEFAULT);
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
        final GetIndexRequest request = new GetIndexRequest(index);
        try {
            return getClient().indices().exists(request, RequestOptions.DEFAULT);
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
            final FlushResponse flushResponse = getClient().indices().flush(request, RequestOptions.DEFAULT);

            final int totalShards = flushResponse.getTotalShards();
            final int successfulShards = flushResponse.getSuccessfulShards();
//            final int failedShards = flushResponse.getFailedShards();
//            final DefaultShardOperationFailedException[] failures = flushResponse.getShardFailures();

            return successfulShards * 100 / totalShards;
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                logger.warn("[{}] index not found to be flushed", index);
            }
        } catch (IllegalStateException exception) {
            logger.warn("[{}] Illegal State!. Message: {}", index, exception.getMessage());
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
            final SyncedFlushResponse flushSyncedResponse = getClient().indices().flushSynced(request, RequestOptions.DEFAULT);

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
            final RefreshResponse refreshResponse = getClient().indices().refresh(request, RequestOptions.DEFAULT);
            return refreshResponse.getTotalShards();
//        } catch (IOException | ElasticsearchStatusException | ResponseException e) {
        } catch (Exception e) {
            logger.warn("Couldn't refresh index [{}]", index);
            logger.warn("Error [{}]", e.getClass());
//            e.printStackTrace();
//            e.printStackTrace();
//            throw new RuntimeException();

        }
        return 0;
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
        connection.close();
    }

    public List<String> getAliases(final String index) {
        final GetAliasesRequest request = new GetAliasesRequest();
        request.indices(index);
        try {
            final GetAliasesResponse response = getClient().indices().getAlias(request, RequestOptions.DEFAULT);
            final Map<String, Set<AliasMetaData>> aliases = response.getAliases();
            return aliases.values().stream().flatMap(Collection::stream).map(AliasMetaData::alias).collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public AcknowledgedResponse updateAlias(final IndicesAliasesRequest.AliasActions aliasAction) {
        final IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(aliasAction);
        try {
            final AcknowledgedResponse indicesAliasesResponse =
                    getClient().indices().updateAliases(request, RequestOptions.DEFAULT);
            return indicesAliasesResponse;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds an alias to an index
     *
     * @param index to be modified
     * @param alias alias to be added
     * @return name of the alias
     */
    public String addAlias(final String index, final String alias) {
        final IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index(index)
                        .alias(alias);
        updateAlias(aliasAction);
        return alias;
    }

    /**
     * Removes an alias to an index
     *
     * @param index to be modified
     * @param alias alias to be removed
     * @return name of the alias
     */
    public String removeAlias(final String index, final String alias) {
        final IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                        .index(index)
                        .alias(alias);
        updateAlias(aliasAction);
        return alias;
    }
}
