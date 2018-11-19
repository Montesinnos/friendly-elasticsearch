package com.montesinnos.friendly.elasticsearch.bulk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BulkMetricsTest {

    @Test
    void incAddedTest() {
        final BulkMetrics bulkMetrics = new BulkMetrics();
        bulkMetrics.incAdded();
        assertEquals(1, bulkMetrics.getRecordsAdded());
    }

    @Test
    void incInsertedTest() {
        final BulkMetrics bulkMetrics = new BulkMetrics();
        bulkMetrics.incInserted(1000);
        bulkMetrics.incInserted(100);
        bulkMetrics.incInserted(10);
        bulkMetrics.incInserted(1);
        assertEquals(1111, bulkMetrics.getRecordsInserted());
    }

    @Test
    void report() {
    }
}