package nju.jgd.planact;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlanActEngineTest {

    private final PlanActEngine engine = new PlanActEngine();

    @Test
    void advancesOneStepAtATimeAndFinishes() {
        PlanActInput input = input();
        PlanActExecution execution = engine.start(input, planDefinition("step-1", "step-2"), 2);

        assertFalse(execution.isComplete());
        assertEquals("step-1", engine.currentStep(execution).getId());

        PlanActExecution afterFirst =
                engine.recordStepResult(
                        execution,
                        new PlanActStepResult("step-1", "lookupOrderStatus", "ok-1", 3L));
        assertFalse(afterFirst.isComplete());
        assertEquals(1, afterFirst.getNextStepIndex());
        assertEquals("step-2", engine.currentStep(afterFirst).getId());

        PlanActExecution afterSecond =
                engine.recordStepResult(
                        afterFirst,
                        new PlanActStepResult("step-2", "lookupOrderStatus", "ok-2", 4L));
        assertTrue(afterSecond.isComplete());
        assertEquals(2, afterSecond.getResults().size());

        PlanActResult result = engine.finish(afterSecond, "done", "test-model");
        assertEquals("task-1", result.getTaskId());
        assertEquals("done", result.getFinalAnswer());
        assertEquals(2, result.getPlan().size());
    }

    @Test
    void rejectsWrongStepResult() {
        PlanActExecution execution = engine.start(input(), planDefinition("step-1"), 1);
        assertThrows(
                PlanActException.class,
                () ->
                        engine.recordStepResult(
                                execution,
                                new PlanActStepResult(
                                        "other", "lookupOrderStatus", "bad", 1L)));
    }

    @Test
    void rejectsTooManyPlannedSteps() {
        assertThrows(
                PlanActException.class,
                () -> engine.start(input(), planDefinition("step-1", "step-2"), 1));
    }

    @Test
    void rejectsFinishBeforeCompletion() {
        PlanActExecution execution = engine.start(input(), planDefinition("step-1"), 1);
        assertThrows(
                PlanActException.class,
                () -> engine.finish(execution, "not yet", "test-model"));
    }

    @Test
    void rejectsDuplicateStepIds() {
        PlanActStep first =
                new PlanActStep(
                        "same",
                        "lookupOrderStatus",
                        Map.of("orderId", "ORD-1001"),
                        "first");
        PlanActStep second =
                new PlanActStep(
                        "same",
                        "lookupOrderStatus",
                        Map.of("orderId", "ORD-1002"),
                        "second");
        assertThrows(
                PlanActException.class,
                () -> engine.start(input(), new PlanActPlan(List.of(first, second)), 2));
    }

    private static PlanActPlan planDefinition(String... ids) {
        List<PlanActStep> steps =
                Arrays.stream(ids)
                        .map(
                                id ->
                                        new PlanActStep(
                                                id,
                                                "lookupOrderStatus",
                                                Map.of("orderId", "ORD-1001"),
                                                id))
                        .toList();
        return new PlanActPlan(steps);
    }

    private static PlanActInput input() {
        return new PlanActInput(
                "task-1", "查询订单", Map.of("userId", "u1"), Map.of("requestId", "r1"));
    }
}