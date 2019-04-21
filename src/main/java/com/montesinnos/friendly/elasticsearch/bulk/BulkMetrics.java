package com.montesinnos.friendly.elasticsearch.bulk;

import com.google.common.base.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.montesinnos.friendly.commons.Pretty.pretty;

/**
 * Holds metrics of a bulk process to report to the user
 *
 * @author montesinnos
 * @since 2018-07-15
 */

public class BulkMetrics {
    private static final Logger logger = LogManager.getLogger(BulkMetrics.class);

    private final AtomicInteger recordsAdded;
    private final AtomicInteger recordsInserted;
    private final Stopwatch stopwatch;
    private int reportSize;

    public BulkMetrics() {
        recordsAdded = new AtomicInteger(0);
        recordsInserted = new AtomicInteger(0);
        this.reportSize = 1000;

        stopwatch = Stopwatch.createStarted();
    }

    public long getRecordsAdded() {
        return recordsAdded.get();
    }

    public long getRecordsInserted() {
        return recordsInserted.get();
    }

    public long getReportSize() {
        return reportSize;
    }

    /**
     * Sets the number of actions in between repost
     *
     * @param reportSize How many actions before any report is printed
     */
    public void setReportSize(int reportSize) {
        this.reportSize = reportSize;
    }

    /**
     * Increments the count of docs being inserted
     *
     * @return new value
     */
    public long incAdded() {
        recordsAdded.incrementAndGet();
        if (recordsAdded.get() % reportSize == 0) { //should report
            report();
        }
        return recordsAdded.get();
    }

    /**
     * Gets the number of seconds since this bulk started working
     *
     * @return seconds
     */
    public long getTime() {
        return Math.max(stopwatch.elapsed(TimeUnit.SECONDS), 1);
    }

    /**
     * Increments the number of documents inserted into Elasticsearch (committed)
     *
     * @param count Delta number to be added
     * @return new value
     */
    public long incInserted(final int count) {
        return recordsInserted.addAndGet(count);
    }

    /**
     * Prints out current numbers
     */
    public void report() {
        final long time = getTime();
        logger.trace("Time {}s" +
                        "\tRecords added: {}\tSpeed: {}docs/s" +
                        "\tRecords Inserted: {}\tSpeed: {}docs/s",
                pretty(time),
                pretty(recordsAdded.get()), pretty(recordsAdded.get() / time),
                pretty(recordsInserted.get()), pretty(recordsInserted.get() / time));
    }
}
