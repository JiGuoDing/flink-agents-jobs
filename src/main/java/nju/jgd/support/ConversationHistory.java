package nju.jgd.support;

import java.util.ArrayList;
import java.util.List;

/** Pure helpers for bounded conversation history. */
public final class ConversationHistory {

    private ConversationHistory() {}

    /** Converts a value restored from memory into a defensive list of strings. */
    public static List<String> coerce(Object rawValue) {
        if (!(rawValue instanceof List<?> values)) {
            return List.of();
        }
        List<String> result = new ArrayList<>(values.size());
        for (Object value : values) {
            if (value != null) {
                result.add(String.valueOf(value));
            }
        }
        return result;
    }

    /**
     * Appends one completed user/assistant turn and keeps only the newest entries.
     *
     * @param existing existing history
     * @param userMessage user message
     * @param assistantMessage assistant answer
     * @param maxEntries maximum retained entries
     * @return bounded history
     */
    public static List<String> appendTurn(
            List<String> existing, String userMessage, String assistantMessage, int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be greater than zero");
        }
        List<String> updated = new ArrayList<>();
        if (existing != null) {
            updated.addAll(existing);
        }
        if (userMessage != null && !userMessage.isBlank()) {
            updated.add("USER: " + userMessage.trim());
        }
        if (assistantMessage != null && !assistantMessage.isBlank()) {
            updated.add("ASSISTANT: " + assistantMessage.trim());
        }
        int fromIndex = Math.max(0, updated.size() - maxEntries);
        return List.copyOf(updated.subList(fromIndex, updated.size()));
    }
}
