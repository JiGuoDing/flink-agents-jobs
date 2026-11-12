package nju.jgd.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Agent 处理后返回给下游的输出消息。
 *
 * <p>该类与 {@link AgentRequest} 使用相同的 Flink POJO 和 Java 序列化约定，确保
 * OutputEvent 进入 DataStream 或 checkpoint 时可以稳定地序列化和反序列化。
 */
public final class AgentResponse implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    /** 该响应所属的会话业务键。 */
    private String sessionId;

    /** Agent 生成的响应内容。 */
    private String message;

    public AgentResponse() {}

    public AgentResponse(String sessionId, String message) {
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

    @Override
    public String toString() {
        return "AgentResponse{sessionId='%s', message='%s'}"
                .formatted(Objects.toString(sessionId), Objects.toString(message));
    }
}
