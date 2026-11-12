package nju.jgd.planact;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlanActStateStoreTest {

    @Test
    void roundTripsExecutionState() throws Exception {
        TestMemoryObject memory = new TestMemoryObject();
        PlanActInput input =
                new PlanActInput(
                        "task-1",
                        "查询订单",
                        Map.of("userId", "u1"),
                        Map.of("requestId", "r1"));
        PlanActExecution execution =
                new PlanActExecution(
                        input,
                        List.of(
                                new PlanActStep(
                                        "step-1",
                                        "lookupOrderStatus",
                                        Map.of("orderId", "ORD-1001"),
                                        "查询")),
                        List.of(new PlanActStepResult("step-1", "lookupOrderStatus", "ok", 2L)),
                        1,
                        1234L);

        PlanActStateStore.save(memory, execution);
        assertTrue(PlanActStateStore.hasState(memory));

        PlanActExecution loaded = PlanActStateStore.load(memory);
        assertEquals("task-1", loaded.getInput().getTaskId());
        assertEquals(1, loaded.getNextStepIndex());
        assertEquals(1, loaded.getResults().size());
        assertEquals("ORD-1001", loaded.getPlan().get(0).getArguments().get("orderId"));
    }

    @Test
    void rejectsMissingState() {
        assertThrows(PlanActException.class, () -> PlanActStateStore.load(new TestMemoryObject()));
    }
}