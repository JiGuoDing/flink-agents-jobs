package nju.jgd.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Agent Job 的输入消息。
 *
 * <p>该类遵循 Flink POJO 约定：public 可实例化类、public 无参构造器、字段通过
 * getter/setter 暴露，并实现 {@link Serializable} 作为序列化兼容边界。
 */
public final class AgentRequest implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    /** 会话业务键，同时作为 keyBy 的 key。 */
    private String sessionId;

    /** 本次会话输入的消息内容。 */
    private String message;

    public AgentRequest() {}

    public AgentRequest(String sessionId, String message) {
        this.sessionId = sessionId;
        this.message = message;
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
}
