package nju.jgd.model;

import java.io.Serial;
import java.io.Serializable;

/** Explicit keyed state maintained for every user. */
public final class UserSessionState implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private long firstSeenAt;
    private long lastSeenAt;
    private long requestCount;
    private String lastSessionId;

    public UserSessionState() {}

    public long getFirstSeenAt() {
        return firstSeenAt;
    }

    public void setFirstSeenAt(long firstSeenAt) {
        this.firstSeenAt = firstSeenAt;
    }

    public long getLastSeenAt() {
        return lastSeenAt;
    }

    public void setLastSeenAt(long lastSeenAt) {
        this.lastSeenAt = lastSeenAt;
    }

    public long getRequestCount() {
        return requestCount;
    }

    public void setRequestCount(long requestCount) {
        this.requestCount = requestCount;
    }

    public String getLastSessionId() {
        return lastSessionId;
    }

    public void setLastSessionId(String lastSessionId) {
        this.lastSessionId = lastSessionId;
    }
}
