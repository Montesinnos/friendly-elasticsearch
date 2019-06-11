package com.montesinnos.friendly.elasticsearch.bulk;

import com.montesinnos.friendly.commons.DateUtils;
import org.apache.logging.log4j.util.Strings;

/**
 * Holds the configuration for the bulk process
 *
 * @author montesinnos
 * @since 2018-07-15
 */
public class BulkConfiguration {

    private final String indexName;
    private final int bulkActions;
    private final int bulkSize;
    private final int flushInterval;
    private final int concurrentRequests;
    private final int reportSize;

    /**
     * Settings for the bulk populator
     *
     * @param indexName          Index to be used
     * @param bulkActions        Docs to be committed
     * @param bulkSize           Size of the bulk in MB to trigger commit
     * @param flushInterval      Time in seconds to trigger commit
     * @param concurrentRequests Threads doing the insert
     * @param reportSize         Interval in docs for the logger
     */
    public BulkConfiguration(final String indexName,
                             int bulkActions, int bulkSize, int flushInterval, final int concurrentRequests, final int reportSize) {
        this.indexName = indexName;
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize;
        this.flushInterval = flushInterval;
        this.concurrentRequests = concurrentRequests;
        this.reportSize = reportSize;

    }

    public int getBulkActions() {
        return bulkActions;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public int getConcurrentRequests() {
        return concurrentRequests;
    }

    public int getReportSize() {
        return reportSize;
    }

    public String getIndexName() {
        return indexName;
    }

    public static class Builder {
        private String indexName;
        private int bulkActions;
        private int bulkSize;
        private int flushInterval;
        private int concurrentRequests;
        private int reportSize;

        public Builder indexName(final String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder bulkActions(final int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        public Builder bulkSize(final int bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        public Builder flushInterval(final int flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder concurrentRequests(final int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        public Builder reportSize(final int reportSize) {
            this.reportSize = reportSize;
            return this;
        }

        public BulkConfiguration build() {

            if (Strings.isBlank(indexName)) {
                indexName = DateUtils.getTimestamp();
            }
            if (bulkActions > 0 && bulkActions < 1_000_000) {
            } else {
                this.bulkActions = 2000;
            }
            if (bulkSize > 1 && bulkSize < 1_000) {
            } else {
                this.bulkSize = 20;
            }
            if (flushInterval > 0 && flushInterval < 1_000) {
            } else {
                this.flushInterval = 100; //Seconds
            }
            if (concurrentRequests > 0 && concurrentRequests < 10) {
            } else {
                this.concurrentRequests = 1;
            }
            if (reportSize > 0 && reportSize < 100_000_000) {
            } else {
                this.reportSize = 10_000;
            }
            return new BulkConfiguration(indexName,
                    bulkActions, bulkSize, flushInterval, concurrentRequests, reportSize);
        }
    }
}