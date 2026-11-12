package nju.jgd.model;

import java.io.Serial;
import java.io.Serializable;

/** Final answer produced by the customer-support agent. */
public final class SupportResponse implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private String requestId;
    private String userId;
    private String sessionId;
    private String answer;
    private boolean cacheHit;
    private int turnNumber;
    private long requestSequence;
    private String model;

    public SupportResponse() {}

    public SupportResponse(
            String requestId,
            String userId,
            String sessionId,
            String answer,
            boolean cacheHit,
            int turnNumber,
            long requestSequence,
            String model) {
        this.requestId = requestId;
        this.userId = userId;
        this.sessionId = sessionId;
        this.answer = answer;
        this.cacheHit = cacheHit;
        this.turnNumber = turnNumber;
        this.requestSequence = requestSequence;
        this.model = model;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getAnswer() {
        return answer;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }

    public boolean isCacheHit() {
        return cacheHit;
    }

    public void setCacheHit(boolean cacheHit) {
        this.cacheHit = cacheHit;
    }

    public int getTurnNumber() {
        return turnNumber;
    }

    public void setTurnNumber(int turnNumber) {
        this.turnNumber = turnNumber;
    }

    public long getRequestSequence() {
        return requestSequence;
    }

    public void setRequestSequence(long requestSequence) {
        this.requestSequence = requestSequence;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    @Override
    public String toString() {
        return ("SupportResponse{requestId='%s', userId='%s', sessionId='%s', cacheHit=%s, "
                        + "turnNumber=%d, requestSequence=%d, model='%s', answer='%s'}")
                .formatted(
                        requestId,
                        userId,
                        sessionId,
                        cacheHit,
                        turnNumber,
                        requestSequence,
                        model,
                        answer);
    }
}

