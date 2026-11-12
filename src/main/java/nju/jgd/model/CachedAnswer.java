package nju.jgd.model;

/** Logical cache entry reconstructed from Flink Agents short-term memory. */
public final class CachedAnswer {

    private final String answer;
    private final String model;
    private final long createdAt;

    public CachedAnswer(String answer, String model, long createdAt) {
        this.answer = answer;
        this.model = model;
        this.createdAt = createdAt;
    }

    public String getAnswer() {
        return answer;
    }

    public String getModel() {
        return model;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
