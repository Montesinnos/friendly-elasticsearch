package com.montesinnos.friendly.elasticsearch.bulk;

import java.nio.file.Path;
import java.util.Collection;

/**
 * Used to do bulk imports
 *
 * @author montesinnos
 * @since 2018-07-15
 */
public interface Bulk {

    /**
     * Inserts a doc into Elasticsearch
     *
     * @param record doc to be inserted
     */
    void insert(final String record);

    /**
     * Inserts a doc into Elasticsearch
     *
     * @param id     for the document
     * @param record doc to be inserted
     */
    void insert(final String id, final String record);

    /**
     * Inserts multiple docs into Elasticsearch
     *
     * @param idField field that contains the id for the doc. Json will be parsed and field value extracted
     * @param records collection of docs to be inserted
     */
    void insert(final String idField, final Collection<?> records);

    /**
     * @param idField field that contains the id for the doc. Json will be parsed and field value extracted
     * @param path    to a file or directory with the docs to insert
     */
    void insert(final String idField, final Path path);

    /**
     * Inserts multiple docs into Elasticsearch
     *
     * @param records collection of docs to be inserted
     */
    void insert(final Collection<?> records);

    void flush();

    void close();
}
