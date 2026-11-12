package nju.jgd.planact;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** A complete, immutable-by-convention Plan-Act plan. */
public final class PlanActPlan implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    private List<PlanActStep> steps = new ArrayList<>();

    public PlanActPlan() {}

    public PlanActPlan(List<PlanActStep> steps) {
        this.steps = steps == null ? new ArrayList<>() : new ArrayList<>(steps);
    }

    public List<PlanActStep> getSteps() {
        if (steps == null) {
            steps = new ArrayList<>();
        }
        return steps;
    }

    public void setSteps(List<PlanActStep> steps) {
        this.steps = steps == null ? new ArrayList<>() : new ArrayList<>(steps);
    }
}