package com.montesinnos.friendly.elasticsearch.bulk;

import com.montesinnos.friendly.elasticsearch.connection.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BulkRunner {
    private static final Logger logger = LogManager.getLogger(BulkRunner.class);
    private final Connection connection;

    public BulkRunner(final Connection connection) {
        this.connection = connection;
    }

    public void populate(final String indexName, final String indexSettings, final String sourcePath) {
        final Path path = Paths.get(sourcePath);

        if (!path.toFile().exists()) {
            throw new RuntimeException("Provided path doesn't exist: " + path);
        }

        final ProcessorBulk processorBulk = new ProcessorBulk(connection);
    }
}
