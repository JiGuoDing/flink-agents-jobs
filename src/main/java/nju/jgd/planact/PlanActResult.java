package nju.jgd.planact;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Successful terminal output of a Plan-Act execution. */
public final class PlanActResult implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private String taskId;
    private String goal;
    private String finalAnswer;
    private List<PlanActStep> plan = new ArrayList<>();
    private List<PlanActStepResult> stepResults = new ArrayList<>();
    private Map<String, String> metadata = new LinkedHashMap<>();
    private String model;
    private long totalDurationMs;

    public PlanActResult() {}

    public PlanActResult(
            String taskId,
            String goal,
            String finalAnswer,
            List<PlanActStep> plan,
            List<PlanActStepResult> stepResults,
            Map<String, String> metadata,
            String model,
            long totalDurationMs) {
        this.taskId = taskId;
        this.goal = goal;
        this.finalAnswer = finalAnswer;
        this.plan = plan == null ? new ArrayList<>() : new ArrayList<>(plan);
        this.stepResults = stepResults == null ? new ArrayList<>() : new ArrayList<>(stepResults);
        this.metadata = metadata == null ? new LinkedHashMap<>() : new LinkedHashMap<>(metadata);
        this.model = model;
        this.totalDurationMs = totalDurationMs;
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

    public String getFinalAnswer() {
        return finalAnswer;
    }

    public void setFinalAnswer(String finalAnswer) {
        this.finalAnswer = finalAnswer;
    }

    public List<PlanActStep> getPlan() {
        if (plan == null) {
            plan = new ArrayList<>();
        }
        return plan;
    }

    public void setPlan(List<PlanActStep> plan) {
        this.plan = plan == null ? new ArrayList<>() : new ArrayList<>(plan);
    }

    public List<PlanActStepResult> getStepResults() {
        if (stepResults == null) {
            stepResults = new ArrayList<>();
        }
        return stepResults;
    }

    public void setStepResults(List<PlanActStepResult> stepResults) {
        this.stepResults = stepResults == null ? new ArrayList<>() : new ArrayList<>(stepResults);
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

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public long getTotalDurationMs() {
        return totalDurationMs;
    }

    public void setTotalDurationMs(long totalDurationMs) {
        this.totalDurationMs = totalDurationMs;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PlanActResult{taskId='").append(taskId).append("', model='").append(model).append("', ")
                .append("totalDurationMs=").append(totalDurationMs).append("}\n");
        builder.append("goal=").append(goal).append('\n');
        builder.append("plan:\n");
        for (int index = 0; index < getPlan().size(); index++) {
            PlanActStep step = getPlan().get(index);
            builder.append("  ").append(index + 1).append(". ").append(step.getId()).append(" -> ")
                    .append(step.getTool()).append('(').append(step.getArguments()).append(")");
            if (!step.getPurpose().isBlank()) {
                builder.append(" : ").append(step.getPurpose());
            }
            builder.append('\n');
        }
        builder.append("stepResults:\n");
        for (PlanActStepResult result : getStepResults()) {
            builder.append("  - ").append(result.getStepId()).append(" [").append(result.getTool())
                    .append("] ").append(result.getDurationMs()).append("ms: ")
                    .append(result.getOutput()).append('\n');
        }
        builder.append("finalAnswer=").append(finalAnswer);
        return builder.toString();
    }
}