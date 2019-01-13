package com.montesinnos.friendly.elasticsearch.index;

public class IndexConfiguration {

    private final String name;
    private final String typeName;
    private final String mapping;

    private final int numberOfShards;
    private final int numberOfReplicas;
    private final int refreshInterval;

    public IndexConfiguration(final String name, final String typeName, final String mapping, final int numberOfShards, final int numberOfReplicas, final int refreshInterval) {
        this.name = name;
        this.typeName = typeName;
        this.mapping = mapping;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.refreshInterval = refreshInterval;
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getMapping() {
        return mapping;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getRefreshInterval() {
        return refreshInterval;
    }

    public static class Builder {

        private String name;
        private String typeName;
        private String mapping;

        private int numberOfShards;
        private int numberOfReplicas;
        private int refreshInterval;

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder typeName(final String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder mapping(final String mapping) {
            this.mapping = mapping;
            return this;
        }

        public Builder numberOfShards(final int numberOfShards) {
            this.numberOfShards = numberOfShards;
            return this;
        }

        public Builder numberOfReplicas(final int numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        public Builder refreshInterval(final int refreshInterval) {
            this.refreshInterval = refreshInterval;
            return this;
        }

        public IndexConfiguration build() {

            return new IndexConfiguration(name, typeName, mapping, numberOfShards, numberOfReplicas, refreshInterval);
        }
    }
}