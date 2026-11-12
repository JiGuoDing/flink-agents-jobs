package nju.jgd.model;

import java.io.Serial;
import java.io.Serializable;

/** Input record processed by the customer-support agent. */
public final class SupportRequest implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private String requestId;
    private String userId;
    private String sessionId;
    private String message;
    private boolean cacheable;
    private long requestSequence;
    private boolean returningUser;

    public SupportRequest() {}

    public SupportRequest(
            String requestId,
            String userId,
            String sessionId,
            String message,
            boolean cacheable) {
        this.requestId = requestId;
        this.userId = userId;
        this.sessionId = sessionId;
        this.message = message;
        this.cacheable = cacheable;
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

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public void setCacheable(boolean cacheable) {
        this.cacheable = cacheable;
    }

    public long getRequestSequence() {
        return requestSequence;
    }

    public void setRequestSequence(long requestSequence) {
        this.requestSequence = requestSequence;
    }

    public boolean isReturningUser() {
        return returningUser;
    }

    public void setReturningUser(boolean returningUser) {
        this.returningUser = returningUser;
    }
}
