package nju.jgd.planact;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** Domain-neutral input accepted by the reusable Plan-Act agent. */
public final class PlanActInput implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private String taskId;
    private String goal;
    private Map<String, String> context = new LinkedHashMap<>();
    private Map<String, String> metadata = new LinkedHashMap<>();

    public PlanActInput() {}

    public PlanActInput(
            String taskId,
            String goal,
            Map<String, String> context,
            Map<String, String> metadata) {
        this.taskId = taskId;
        this.goal = goal;
        this.context = context == null ? new LinkedHashMap<>() : new LinkedHashMap<>(context);
        this.metadata = metadata == null ? new LinkedHashMap<>() : new LinkedHashMap<>(metadata);
    }

    public void validate() {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (goal == null || goal.isBlank()) {
            throw new IllegalArgumentException("goal must not be blank");
        }
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getGoal() {
        return goal;
    }

    public void setGoal(String goal) {
        this.goal = goal;
    }

    public Map<String, String> getContext() {
        if (context == null) {
            context = new LinkedHashMap<>();
        }
        return context;
    }

    public void setContext(Map<String, String> context) {
        this.context = context == null ? new LinkedHashMap<>() : new LinkedHashMap<>(context);
    }

    public Map<String, String> getMetadata() {
        if (metadata == null) {
            metadata = new LinkedHashMap<>();
        }
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata == null ? new LinkedHashMap<>() : new LinkedHashMap<>(metadata);
    }
}