package com.montesinnos.friendly.elasticsearch.bulk;

/**
 * Used to hold data before inserting in Elasticsearch
 *
 * @author montesinnos
 * @since 2018-07-15
 */
public class BulkItem {

    private final String id;
    private final String doc;

    public BulkItem(String id, String doc) {
        this.id = id;
        this.doc = doc;
    }

    public String getId() {
        return id;
    }

    public String getDoc() {
        return doc;
    }
}
