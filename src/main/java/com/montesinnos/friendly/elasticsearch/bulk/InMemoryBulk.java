package com.montesinnos.friendly.elasticsearch.bulk;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.montesinnos.friendly.commons.Pretty;
import com.montesinnos.friendly.commons.file.FileUtils;
import com.montesinnos.friendly.commons.file.TextFileUtils;
import com.montesinnos.friendly.elasticsearch.client.FriendlyClient;
import com.montesinnos.friendly.elasticsearch.connection.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryBulk implements Bulk {
    private static final Logger logger = LogManager.getLogger(InMemoryBulk.class);
    private final Connection connection;
    private final BulkMetrics bulkMetrics;
    private final BulkConfiguration bulkConfiguration;
    private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            int numberOfActions = request.numberOfActions();
//            logger.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            bulkMetrics.incInserted(numberOfActions);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request,
                              BulkResponse response) {
            if (response.hasFailures()) {
                logger.warn("ProcessorBulk [{}] executed with failures", executionId);
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
//                logger.debug("ProcessorBulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Failed to execute bulk [{}]", executionId, failure);
        }
    };


    public InMemoryBulk(final Connection connection) {
        this(connection, new BulkConfiguration.Builder().build());
    }

    public InMemoryBulk(final Connection connection, final BulkConfiguration bulkConfiguration) {
        this.connection = connection;
        this.bulkConfiguration = bulkConfiguration;
        bulkMetrics = new BulkMetrics();
        bulkMetrics.setReportSize(bulkConfiguration.getReportSize());
    }

    /**
     * Gets a brand new bulk processor ready to used as a inserter
     *
     * @return new ProcessorBulk Processor
     */
    private BulkProcessor getBulkProcessor() {
        final BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) -> connection.refresh().getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
        builder.setBulkActions(bulkConfiguration.getBulkActions() + 10);
        builder.setBulkSize(new ByteSizeValue(bulkConfiguration.getBulkSize(), ByteSizeUnit.MB));
        builder.setConcurrentRequests(bulkConfiguration.getConcurrentRequests());
        builder.setFlushInterval(TimeValue.timeValueSeconds(bulkConfiguration.getFlushInterval()));
        builder.setBackoffPolicy(BackoffPolicy
                .exponentialBackoff(TimeValue.timeValueSeconds(2L), 3));

        final BulkProcessor bulkProcessor = builder.build();
        return bulkProcessor;
    }

    private long inc() {
        if (docs.size() >= bulkConfiguration.getBulkActions()) {
            flush();
        }
        return bulkMetrics.incAdded();
    }

    private final Collection<BulkItem> docs = new ArrayList<>();
    private final AtomicInteger bulkCount = new AtomicInteger();

    /**
     * Inserts a doc into Elasticsearch
     *
     * @param id     for the document
     * @param record doc to be inserted
     */
    @Override
    public void insert(final String id, final String record) {
        docs.add(new BulkItem(id, record));
        inc();
    }

    /**
     * Inserts a doc into Elasticsearch
     *
     * @param record doc to be inserted
     */
    @Override
    public void insert(final String record) {
        docs.add(new BulkItem(null, record));
        inc();
    }

    /**
     * Inserts multiple docs into Elasticsearch
     *
     * @param records collection of docs to be inserted
     */
    @Override
    public void insert(final Collection<?> records) {
        final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

        records.forEach(record -> {

            try {
                docs.add(new BulkItem(null, objectMapper.writeValueAsString(record)));
                inc();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Inserts multiple docs into Elasticsearch
     *
     * @param idField field that contains the id for the doc. Json will be parsed and field value extracted
     * @param records collection of docs to be inserted
     */
    @Override
    public void insert(final String idField, final Collection<?> records) {
        final ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

        records.forEach(record -> {
            try {
                final String json = objectMapper.writeValueAsString(record);
                final String id = objectMapper.readTree(json).get(idField).asText();

                docs.add(new BulkItem(id, json));
                inc();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * @param idField field that contains the id for the doc. Json will be parsed and field value extracted
     * @param path    to a file or directory with the docs to insert
     */
    @Override
    public void insert(final String idField, final Path path) {
        FileUtils.getFiles(path)
                .forEach(x -> {
                    TextFileUtils.readLines(x)
                            .forEach(line -> insert(idField, line));
                });
    }

    /**
     * Flushes the bulk processor, committing current docs
     */
    @Override
    public void flush() {
        if (docs.size() > 0) {

            final int currentBulk = bulkCount.incrementAndGet();
            logger.trace("Executing bulk [{}] with {} requests",
                    currentBulk, Pretty.pretty(docs.size()));

            final long startTime = System.currentTimeMillis();

            final BulkProcessor bulkProcessor = getBulkProcessor();
            docs.forEach(doc ->
            {
                if (Strings.isBlank(doc.getId())) {
                    bulkProcessor.add(
                            new IndexRequest(bulkConfiguration.getIndexName(), bulkConfiguration.getTypeName()).
                                    source(doc.getDoc(), XContentType.JSON));
                } else {
                    bulkProcessor.add(
                            new IndexRequest(bulkConfiguration.getIndexName(), bulkConfiguration.getTypeName(), doc.getId()).
                                    source(doc.getDoc(), XContentType.JSON));

                }
            });
            bulkProcessor.flush();
            try {
                bulkProcessor.awaitClose(3, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            docs.clear();

            logger.trace("Bulk [{}] completed in {} milliseconds",
                    currentBulk, Pretty.pretty(System.currentTimeMillis() - startTime));
        }
    }

    /**
     * Flushes and refreshes the ProcessorBulk
     */
    public void refresh() {
        new FriendlyClient(connection).refresh();
    }

    /**
     * Gets current number of docs inserted
     *
     * @return number of docs inserted
     */
    public long getRecordsAdded() {
        return bulkMetrics.getRecordsAdded();
    }

    @Override
    public void close() {
        flush();
        refresh();
        connection.close();
        bulkMetrics.report();
    }
}
