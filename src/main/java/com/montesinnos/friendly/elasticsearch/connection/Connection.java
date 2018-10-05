package com.montesinnos.friendly.elasticsearch.connection;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;

import java.io.IOException;


public class Connection {
    private static final Logger logger = LogManager.getLogger(Connection.class);
    private final RestHighLevelClient client;

    public Connection() {
        this("localhost", 9200, "http");
    }

    public Connection(final String host, final int port, final String protocol) {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, protocol)));
        checkInfo();
    }

    public Connection(final String host, final int port, final String protocol, final HttpRequestInterceptor interceptor) {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, protocol))
                        .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)));
        checkInfo();
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

    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
