package com.montesinnos.friendly.elasticsearch.search;

public class Metadata {
    final Long hits;

    final Float maxScore;

    public Metadata(Long hits, Float maxScore) {
        this.hits = hits;
        this.maxScore = maxScore;
    }

    public Long getHits() {
        return hits;
    }

    public Float getMaxScore() {
        return maxScore;
    }
}
