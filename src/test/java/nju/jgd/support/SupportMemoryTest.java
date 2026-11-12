package nju.jgd.support;

import nju.jgd.model.CachedAnswer;
import org.apache.flink.agents.api.context.MemoryObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class SupportMemoryTest {

    @Test
    void cacheIsSessionIndependentButUserMemoryIsIsolated() throws Exception {
        MemoryObject userOne = new TestMemoryObject();
        MemoryObject userTwo = new TestMemoryObject();
        String cacheKey = CacheKeys.cacheKey("退货政策是什么？", "gpt-4o-mini", "v1");

        SupportMemory.writeCachedAnswer(userOne, cacheKey, "七天无理由退货", "gpt-4o-mini", 1_000L);

        CachedAnswer cached = SupportMemory.readCachedAnswer(userOne, cacheKey, 1_500L, 1_000L);
        assertNotNull(cached);
        assertEquals("七天无理由退货", cached.getAnswer());
        assertNull(SupportMemory.readCachedAnswer(userTwo, cacheKey, 1_500L, 1_000L));
    }

    @Test
    void expiredCacheEntryIsIgnored() throws Exception {
        MemoryObject memory = new TestMemoryObject();
        String cacheKey = CacheKeys.cacheKey("退货政策是什么？", "gpt-4o-mini", "v1");
        SupportMemory.writeCachedAnswer(memory, cacheKey, "旧答案", "gpt-4o-mini", 1_000L);

        assertNull(SupportMemory.readCachedAnswer(memory, cacheKey, 2_001L, 1_000L));
    }

    @Test
    void sessionHistoryAndTurnCountersAreIsolated() throws Exception {
        MemoryObject memory = new TestMemoryObject();

        assertEquals(1, SupportMemory.recordTurn(memory, "s-1", "问题一", "回答一"));
        assertEquals(1, SupportMemory.recordTurn(memory, "s-2", "问题二", "回答二"));
        assertEquals(2, SupportMemory.recordTurn(memory, "s-1", "问题三", "回答三"));

        assertEquals(
                List.of("USER: 问题一", "ASSISTANT: 回答一", "USER: 问题三", "ASSISTANT: 回答三"),
                SupportMemory.readHistory(memory, "s-1"));
        assertEquals(
                List.of("USER: 问题二", "ASSISTANT: 回答二"),
                SupportMemory.readHistory(memory, "s-2"));
        assertNotEquals(
                SupportMemory.historyPath("s-1"), SupportMemory.historyPath("s-2"));
    }

    @Test
    void historyIsBounded() throws Exception {
        MemoryObject memory = new TestMemoryObject();
        for (int index = 0; index < 6; index++) {
            SupportMemory.recordTurn(memory, "s-1", "q" + index, "a" + index);
        }

        assertEquals(SupportMemory.MAX_HISTORY_ENTRIES, SupportMemory.readHistory(memory, "s-1").size());
    }
}
