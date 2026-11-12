package nju.jgd.support;

import org.junit.jupiter.api.Test;

import nju.jgd.model.SupportResponse;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConversationHistoryTest {

    @Test
    void keepsOnlyNewestEntries() {
        List<String> history =
                ConversationHistory.appendTurn(
                        List.of("USER: old", "ASSISTANT: old answer"),
                        "new question",
                        "new answer",
                        3);

        assertEquals(List.of("ASSISTANT: old answer", "USER: new question", "ASSISTANT: new answer"), history);
    }

    @Test
    void supportResponseToStringUsesNumericArgumentsCorrectly() {
        SupportResponse response =
                new SupportResponse("r1", "u1", "s1", "answer", false, 2, 7L, "model");

        assertTrue(response.toString().contains("turnNumber=2"));
        assertTrue(response.toString().contains("requestSequence=7"));
        assertTrue(response.toString().contains("answer='answer'"));
    }

    @Test
    void cacheTtlUsesTimestampBoundary() {
        assertTrue(CacheTtl.isFresh(1_000L, 1_999L, 1_000L));
        assertTrue(CacheTtl.isFresh(1_000L, 2_000L, 1_000L));
        assertFalse(CacheTtl.isFresh(1_000L, 2_001L, 1_000L));
        assertFalse(CacheTtl.isFresh(0L, 1_000L, 1_000L));
    }
}

