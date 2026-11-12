package nju.jgd.support;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheKeysTest {

    @Test
    void normalizesWhitespaceAndCase() {
        assertEquals("hello world", CacheKeys.normalizeMessage("  HELLO   World  "));
    }

    @Test
    void cacheKeyIsStableAndScopedByModelAndPromptVersion() {
        String first = CacheKeys.cacheKey(" 退货 政策？ ", "gpt-4o-mini", "v1");
        String second = CacheKeys.cacheKey("退货 政策？", "gpt-4o-mini", "v1");

        assertEquals(first, second);
        assertNotEquals(first, CacheKeys.cacheKey("退货 政策？", "another-model", "v1"));
        assertNotEquals(first, CacheKeys.cacheKey("退货 政策？", "gpt-4o-mini", "v2"));
        assertTrue(first.matches("[0-9a-f]{64}"));
    }

    @Test
    void safeSegmentKeepsReadableIdsAndEscapesDots() {
        assertEquals("session-1", CacheKeys.safeSegment("session-1"));
        assertTrue(CacheKeys.safeSegment("session.with.dot").startsWith("h_"));
    }
}
