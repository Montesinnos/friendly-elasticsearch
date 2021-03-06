package com.montesinnos.friendly.elasticsearch.document;

import com.montesinnos.friendly.elasticsearch.Wrapper;
import org.elasticsearch.action.DocWriteResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ImmediateDocumentTest {
    private final Document document = DocumentFactory.immediate(Wrapper.getConnection().getClient());
    private final String index = "document_test_index";

    @Test
    void insertTest() {
        document.insert(index, "{}");
        final String insert_id_1 = document.insert(index, "insert_id_1", "{}");
        assertEquals("insert_id_1", insert_id_1);
    }

    @Test
    void updateTest() {
        document.insert(index, "{}");
        document.insert(index, "update_id_1", "{}");
        final String update = document.update(index, "update_id_1", "{\"id\":1}");
        assertEquals("UPDATED", update);
    }

    @Test
    void updateAndGetTest() {
        document.insert(index, "{}");
        document.insert(index, "update_and_get_id_1", "{\"field\":2}");
        final String update = document.updateAndGet(index, "update_and_get_id_1", "{\"id\":1}");
        assertEquals("{\"field\":2,\"id\":1}", update);
    }

    @Test
    void deleteTest() {
        document.insert(index, "delete_id_1", "{}");
        final DocWriteResponse.Result result = document.delete(index, "delete_id_1");
        assertEquals(DocWriteResponse.Result.DELETED, result);
        final DocWriteResponse.Result result2 = document.delete(index, "delete_id_non_existing");
        assertEquals(DocWriteResponse.Result.NOT_FOUND, result2);
    }
}