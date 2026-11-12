package nju.jgd.planact;

import java.io.Serial;
import java.io.Serializable;

/** Result of one successfully executed Plan-Act step. */
public final class PlanActStepResult implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private String stepId;
    private String tool;
    private String output = "";
    private long durationMs;

    public PlanActStepResult() {}

    public PlanActStepResult(String stepId, String tool, String output, long durationMs) {
        this.stepId = stepId;
        this.tool = tool;
        this.output = output == null ? "" : output;
        this.durationMs = durationMs;
    }

    public String getStepId() {
        return stepId;
    }

    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public String getTool() {
        return tool;
    }

    public void setTool(String tool) {
        this.tool = tool;
    }

    public String getOutput() {
        return output == null ? "" : output;
    }

    public void setOutput(String output) {
        this.output = output == null ? "" : output;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }
}