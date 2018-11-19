package com.montesinnos.friendly.elasticsearch.search;

import java.util.List;
import java.util.Map;

public class SearchResults {
    final List<Map<String, Object>> data;
    final Metadata metadata;

    public SearchResults(List<Map<String, Object>> data, Metadata meda) {
        this.data = data;
        this.metadata = meda;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
