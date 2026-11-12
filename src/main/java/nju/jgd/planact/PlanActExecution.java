package nju.jgd.planact;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Checkpointable Plan-Act execution state. */
public final class PlanActExecution implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private PlanActInput input;
    private List<PlanActStep> plan = new ArrayList<>();
    private List<PlanActStepResult> results = new ArrayList<>();
    private int nextStepIndex;
    private long startedAtMillis;

    public PlanActExecution() {}

    public PlanActExecution(
            PlanActInput input,
            List<PlanActStep> plan,
            List<PlanActStepResult> results,
            int nextStepIndex,
            long startedAtMillis) {
        this.input = input;
        this.plan = plan == null ? new ArrayList<>() : new ArrayList<>(plan);
        this.results = results == null ? new ArrayList<>() : new ArrayList<>(results);
        this.nextStepIndex = nextStepIndex;
        this.startedAtMillis = startedAtMillis;
    }

    public PlanActInput getInput() {
        return input;
    }

    public void setInput(PlanActInput input) {
        this.input = input;
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

    public List<PlanActStepResult> getResults() {
        if (results == null) {
            results = new ArrayList<>();
        }
        return results;
    }

    public void setResults(List<PlanActStepResult> results) {
        this.results = results == null ? new ArrayList<>() : new ArrayList<>(results);
    }

    public int getNextStepIndex() {
        return nextStepIndex;
    }

    public void setNextStepIndex(int nextStepIndex) {
        this.nextStepIndex = nextStepIndex;
    }

    public long getStartedAtMillis() {
        return startedAtMillis;
    }

    public void setStartedAtMillis(long startedAtMillis) {
        this.startedAtMillis = startedAtMillis;
    }

    @JsonIgnore
    public boolean isComplete() {
        return nextStepIndex >= getPlan().size();
    }
}