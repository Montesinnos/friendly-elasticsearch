package com.montesinnos.friendly.elasticsearch.connection;

import com.google.common.base.Stopwatch;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Creates a connection to Elasticsearch
 * It now has an option to refresh (re-open) the connection because it was having issues with signatures
 * You can create a new connection every time you need. It doesn't seem like a great idea, but beats having
 * invalid signatures
 *
 * @author montesinnos
 * @since 2018-07-15
 */
public class Connection {
    private static final Logger logger = LogManager.getLogger(Connection.class);
    private RestHighLevelClient client;
    private boolean isOpen = false;
    private final RestClientBuilder restClientBuilder;

    public Connection() {
        this("localhost", 9200, "http");
    }

    public Connection(final String host, final int port, final String protocol) {
        final Stopwatch timer = Stopwatch.createStarted();
        restClientBuilder = RestClient.builder(
                new HttpHost(host, port, protocol));
        connect();
        logger.info("Connected to {} in {}ms", host, timer.elapsed(TimeUnit.MILLISECONDS));
    }

    public Connection(final String host, final int port, final String protocol, final HttpRequestInterceptor interceptor) {
        final Stopwatch timer = Stopwatch.createStarted();
        restClientBuilder = RestClient.builder(
                new HttpHost(host, port, protocol))
                .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor));
        connect();
        logger.info("Connected to {} in {}ms", host, timer.elapsed(TimeUnit.MILLISECONDS));
    }

    public void checkInfo() {
        try {
            final MainResponse response = client.info(RequestOptions.DEFAULT);
            final ClusterName clusterName = response.getClusterName();
            final String clusterUuid = response.getClusterUuid();
            final String nodeName = response.getNodeName();
            final Version version = response.getVersion();
            final Build build = response.getBuild();
            logger.info("Elasticsearch connection successful! Cluster name: {} - Version {}", clusterName, version);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error connecting to Elasticsearch.");
        }
    }

    /**
     * Refreshes the current connection
     *
     * @return this Connection so it can be linked
     */
    public Connection refresh() {
        connect();
        isOpen = true;
        return this;
    }

    /**
     * Connects the client with connection from the Builder
     */
    private void connect() {
        close(client);
        client = new RestHighLevelClient(restClientBuilder);

    }

    private void close(final RestHighLevelClient client) {
        if (client != null) {
            try {
                isOpen = false;
                client.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        close(client);
    }

    public RestHighLevelClient getClient() {
        if (!isOpen) {
            connect();
        }
        return client;
    }
}
