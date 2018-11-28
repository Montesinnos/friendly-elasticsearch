package com.montesinnos.friendly.elasticsearch.bulk;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.montesinnos.friendly.commons.file.FileUtils;
import com.montesinnos.friendly.commons.file.TextFileUtils;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class Bulk {
    private static final Logger logger = LogManager.getLogger(Bulk.class);
    private final FriendlyClient client;
    private final BulkProcessor bulkProcessor;
    private final BulkMetrics bulkMetrics;
    private final BulkConfiguration bulkConfiguration;


    private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            int numberOfActions = request.numberOfActions();
            logger.debug("Executing bulk [{}] with {} requests",
                    executionId, numberOfActions);
            bulkMetrics.incInserted(numberOfActions);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request,
                              BulkResponse response) {
            if (response.hasFailures()) {
                logger.warn("Bulk [{}] executed with failures", executionId);
//
//                BulkItemResponse[] responses = response.getItems();
//                for (int i = 0; i < responses.length; i++) {
//                    BulkItemResponse r = responses[i];
//                    if (r.isFailed()) {
//
//                        System.out.println(request.getDescription());
//                    }
//                }

                logger.warn(response.buildFailureMessage());
            } else {
                logger.debug("Bulk [{}] completed in {} milliseconds",
                        executionId, response.getTook().getMillis());
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Failed to execute bulk [{}]", executionId, failure);
        }
    };


    public Bulk(final RestHighLevelClient client) {
        this(client, new BulkConfiguration.Builder().build());
    }

    public Bulk(final RestHighLevelClient client, final BulkConfiguration bulkConfiguration) {
        this.client = new FriendlyClient(client);
        this.bulkConfiguration = bulkConfiguration;
        final BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
        builder.setBulkActions(bulkConfiguration.getBulkActions());
        builder.setBulkSize(new ByteSizeValue(bulkConfiguration.getBulkSize(), ByteSizeUnit.MB));
        builder.setConcurrentRequests(bulkConfiguration.getConcurrentRequests());
        builder.setFlushInterval(TimeValue.timeValueSeconds(bulkConfiguration.getFlushInterval()));
        builder.setBackoffPolicy(BackoffPolicy
                .exponentialBackoff(TimeValue.timeValueSeconds(2L), 3));

        bulkProcessor = builder.build();
        bulkMetrics = new BulkMetrics();
        bulkMetrics.setReportSize(bulkConfiguration.getReportSize());
    }


    public void insert(final String index, final String type, final String id, final String record) {
        bulkProcessor.add(
                new IndexRequest(index, type, id).
                        source(record, XContentType.JSON));
        inc();
    }

    private long inc() {
        return bulkMetrics.incAdded();
    }


    /**
     * Inserts multiple docs into Elasticsearch
     *
     * @param index  index name to be used
     * @param type   type name to be used
     * @param record doc to be inserted
     */
    public void insert(final String index, final String type, final String record) {
        bulkProcessor.add(
                new IndexRequest(index, type).
                        source(record, XContentType.JSON));
        inc();
    }

    /**
     * Inserts multiple docs into Elasticsearch
     *
     * @param index   index name to be used
     * @param type    type name to be used
     * @param records collection of docs to be inserted
     */
    public void insert(final String index, final String type, final Collection<?> records) {
        final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

        records.forEach(record -> {
            try {
                bulkProcessor.add(
                        new IndexRequest(index, type).
                                source(objectMapper.writeValueAsString(record), XContentType.JSON));
                inc();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Inserts multiple docs into Elasticsearch
     *
     * @param index   index name to be used
     * @param type    type name to be used
     * @param idField field that contains the id for the doc. Json will be parsed and field value extracted
     * @param records collection of docs to be inserted
     */
    public void insert(final String index, final String type, final String idField, final Collection<?> records) {
        final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

        records.forEach(record -> {
            try {
                final String json = objectMapper.writeValueAsString(record);
                final String id = objectMapper.readTree(json).get(idField).asText();
                bulkProcessor.add(
                        new IndexRequest(index, type, id).
                                source(json, XContentType.JSON));
                inc();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * @param index   index name to be used
     * @param type    type name to be used
     * @param idField field that contains the id for the doc. Json will be parsed and field value extracted
     * @param path    to a file or directory with the records to insert
     */
    public void insert(final String index, final String type, final String idField, final Path path) {
        FileUtils.getFiles(path)
                .forEach(x -> {
                    TextFileUtils.readLines(x)
                            .forEach(line -> insert(index, type, idField, line));
                });
    }

    /**
     * Flushes the bulk processor, committing current records
     */
    public void flush() {
        bulkProcessor.flush();
    }

    /**
     * Gets current number of records inserted
     *
     * @return number of records inserted
     */
    public long getRecordsAdded() {
        return bulkMetrics.getRecordsAdded();
    }

    public void close() {
        bulkProcessor.flush();
        client.refresh();
        client.flush();
        try {
            boolean terminated = bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
            logger.info("Bulk Processor closing. Terminated: {}", terminated);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
