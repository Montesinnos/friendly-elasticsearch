package com.montesinnos.friendly.elasticsearch.document;

import com.montesinnos.friendly.elasticsearch.Wrapper;
import org.elasticsearch.action.DocWriteResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DocumentTest {
    final Document document = new Document(Wrapper.getConnection().getClient());
    final String index = "Document_test_index";
    final String type = "Document_test_type";

    @Test
    void insertTest() {
        document.insert(index, type, "{}");
        final String insert_id_1 = document.insert(index, type, "insert_id_1", "{}");
        assertEquals("insert_id_1", insert_id_1);
    }

    @Test
    void updateTest() {
    }

    @Test
    void deleteTest() {
        document.insert(index, type, "delete_id_1", "{}");
        final DocWriteResponse.Result result = document.delete(index, type, "delete_id_1");
        assertEquals(DocWriteResponse.Result.DELETED, result);
        final DocWriteResponse.Result result2 = document.delete(index, type, "delete_id_non_existing");
        assertEquals(DocWriteResponse.Result.NOT_FOUND, result2);

    }
}