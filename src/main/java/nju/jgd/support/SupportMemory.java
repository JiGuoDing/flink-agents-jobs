package nju.jgd.support;

import nju.jgd.model.CachedAnswer;
import org.apache.flink.agents.api.context.MemoryObject;

import java.util.List;

/**
 * Namespaced access to the Flink Agents short-term memory.
 *
 * <p>The agent operator is keyed by userId, so the same memory root is shared by all sessions of
 * one user. Session history remains isolated below the session-specific path.
 */
public final class SupportMemory {

    public static final int MAX_HISTORY_ENTRIES = 8;

    private SupportMemory() {}

    public static List<String> readHistory(MemoryObject memory, String sessionId) throws Exception {
        String path = historyPath(sessionId);
        if (!memory.isExist(path)) {
            return List.of();
        }
        return ConversationHistory.coerce(memory.get(path).getValue());
    }

    /** Adds one completed turn and returns the resulting turn number for this session. */
    public static int recordTurn(
            MemoryObject memory, String sessionId, String userMessage, String answer)
            throws Exception {
        int turnNumber = completedTurns(memory, sessionId) + 1;
        List<String> history =
                ConversationHistory.appendTurn(
                        readHistory(memory, sessionId),
                        userMessage,
                        answer,
                        MAX_HISTORY_ENTRIES);
        memory.set(historyPath(sessionId), history);
        memory.set(turnCountPath(sessionId), turnNumber);
        return turnNumber;
    }

    public static CachedAnswer readCachedAnswer(
            MemoryObject memory, String cacheKey, long now, long ttlMillis) throws Exception {
        String answerPath = cachePath(cacheKey) + ".answer";
        String createdAtPath = cachePath(cacheKey) + ".createdAt";
        if (!memory.isExist(answerPath) || !memory.isExist(createdAtPath)) {
            return null;
        }

        Object answerValue = memory.get(answerPath).getValue();
        Object createdAtValue = memory.get(createdAtPath).getValue();
        if (!(answerValue instanceof String answer)
                || answer.isBlank()
                || !(createdAtValue instanceof Number createdAtNumber)) {
            return null;
        }

        long createdAt = createdAtNumber.longValue();
        if (!CacheTtl.isFresh(createdAt, now, ttlMillis)) {
            return null;
        }

        String modelPath = cachePath(cacheKey) + ".model";
        String model =
                memory.isExist(modelPath)
                        ? String.valueOf(memory.get(modelPath).getValue())
                        : "unknown";
        return new CachedAnswer(answer, model, createdAt);
    }

    public static void writeCachedAnswer(
            MemoryObject memory, String cacheKey, String answer, String model, long createdAt)
            throws Exception {
        if (answer == null || answer.isBlank()) {
            return;
        }
        String root = cachePath(cacheKey);
        memory.set(root + ".answer", answer);
        memory.set(root + ".model", model == null || model.isBlank() ? "unknown" : model);
        memory.set(root + ".createdAt", createdAt);
    }

    public static String historyPath(String sessionId) {
        return sessionRoot(sessionId) + ".history";
    }

    public static String turnCountPath(String sessionId) {
        return sessionRoot(sessionId) + ".turnCount";
    }

    public static String cachePath(String cacheKey) {
        return "cache." + CacheKeys.safeSegment(cacheKey);
    }

    private static String sessionRoot(String sessionId) {
        return "sessions." + CacheKeys.safeSegment(sessionId);
    }

    private static int completedTurns(MemoryObject memory, String sessionId) throws Exception {
        String path = turnCountPath(sessionId);
        if (!memory.isExist(path)) {
            return 0;
        }
        Object value = memory.get(path).getValue();
        return value instanceof Number number ? number.intValue() : 0;
    }
}
