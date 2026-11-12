package nju.jgd.planact;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Pure state machine for a fixed, sequential Plan-Act execution. */
public final class PlanActEngine {

    public PlanActExecution start(PlanActInput input, PlanActPlan plan, int maxSteps) {
        if (input == null) {
            throw new PlanActException("Plan-Act input must not be null");
        }
        input.validate();
        if (maxSteps <= 0) {
            throw new PlanActException("maxSteps must be greater than zero");
        }
        if (plan == null || plan.getSteps().isEmpty()) {
            throw new PlanActException("Plan-Act plan must contain at least one step");
        }
        if (plan.getSteps().size() > maxSteps) {
            throw new PlanActException(
                    "Plan-Act plan contains " + plan.getSteps().size()
                            + " steps, exceeding maxSteps=" + maxSteps);
        }

        Set<String> stepIds = new HashSet<>();
        for (PlanActStep step : plan.getSteps()) {
            if (step == null) {
                throw new PlanActException("Plan-Act plan contains a null step");
            }
            step.validate();
            if (!stepIds.add(step.getId())) {
                throw new PlanActException("Duplicate Plan-Act step id: " + step.getId());
            }
        }
        return new PlanActExecution(
                input, plan.getSteps(), new ArrayList<>(), 0, System.currentTimeMillis());
    }

    public PlanActStep currentStep(PlanActExecution execution) {
        validateExecution(execution);
        if (execution.isComplete()) {
            throw new PlanActException(
                    "Plan-Act execution for task '" + execution.getInput().getTaskId()
                            + "' has no current step");
        }
        return execution.getPlan().get(execution.getNextStepIndex());
    }

    public PlanActExecution recordStepResult(
            PlanActExecution execution, PlanActStepResult result) {
        PlanActStep current = currentStep(execution);
        if (result == null) {
            throw new PlanActException("Plan-Act step result must not be null");
        }
        if (!current.getId().equals(result.getStepId())) {
            throw new PlanActException(
                    "Step result '" + result.getStepId() + "' does not match expected step '"
                            + current.getId() + "'");
        }
        if (!current.getTool().equals(result.getTool())) {
            throw new PlanActException(
                    "Step result tool '" + result.getTool() + "' does not match planned tool '"
                            + current.getTool() + "'");
        }

        List<PlanActStepResult> results = new ArrayList<>(execution.getResults());
        results.add(result);
        return new PlanActExecution(
                execution.getInput(),
                execution.getPlan(),
                results,
                execution.getNextStepIndex() + 1,
                execution.getStartedAtMillis());
    }

    public PlanActResult finish(PlanActExecution execution, String finalAnswer, String model) {
        validateExecution(execution);
        if (!execution.isComplete()) {
            throw new PlanActException(
                    "Cannot finish Plan-Act execution for task '" + execution.getInput().getTaskId()
                            + "' before all steps complete");
        }
        if (finalAnswer == null || finalAnswer.isBlank()) {
            throw new PlanActException("Plan-Act final answer must not be blank");
        }
        long totalDurationMs = Math.max(0L, System.currentTimeMillis() - execution.getStartedAtMillis());
        return new PlanActResult(
                execution.getInput().getTaskId(),
                execution.getInput().getGoal(),
                finalAnswer,
                execution.getPlan(),
                execution.getResults(),
                execution.getInput().getMetadata(),
                model,
                totalDurationMs);
    }

    private static void validateExecution(PlanActExecution execution) {
        if (execution == null) {
            throw new PlanActException("Plan-Act execution must not be null");
        }
        if (execution.getInput() == null) {
            throw new PlanActException("Plan-Act execution is missing its input");
        }
        execution.getInput().validate();
        if (execution.getPlan().isEmpty()) {
            throw new PlanActException("Plan-Act execution is missing its plan");
        }
        if (execution.getNextStepIndex() < 0 || execution.getNextStepIndex() > execution.getPlan().size()) {
            throw new PlanActException(
                    "Plan-Act nextStepIndex " + execution.getNextStepIndex()
                            + " is outside the plan");
        }
    }
}