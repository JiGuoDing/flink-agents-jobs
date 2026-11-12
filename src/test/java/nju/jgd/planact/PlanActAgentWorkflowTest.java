package nju.jgd.planact;

import nju.jgd.tool.support.OrderTools;

import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.resource.Resource;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.integrations.chatmodels.openai.OpenAICompletionsSetup;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlanActAgentWorkflowTest {

    @Test
    void runsPlanStepAndSynthesisWithoutLiveModel() throws Exception {
        PlanActTool tool =
                PlanActTool.fromStaticMethod(
                        OrderTools.class.getMethod("lookupOrderStatus", String.class));
        PlanActAgent agent =
                PlanActAgent.builder()
                        .chatModel(testModel())
                        .tool(tool)
                        .maxSteps(3)
                        .build();
        Map<String, Object> config = agent.getActions().get("plan").f2;
        PlanActInput input =
                new PlanActInput(
                        "task-1",
                        "查询订单 ORD-1001 的物流状态",
                        Map.of("userId", "u1"),
                        Map.of("requestId", "r1"));
        ScriptedRunnerContext context =
                new ScriptedRunnerContext(
                        config,
                        Map.<String, Resource>of("lookupOrderStatus", tool),
                        List.of(
                                """
                                {"steps":[{"id":"step-1","tool":"lookupOrderStatus","arguments":{"orderId":"ORD-1001"},"purpose":"确认物流状态"}]}
                                """,
                                "订单 ORD-1001 已发货。"));

        PlanActAgent.plan(new InputEvent(input), context);
        Event planReady = context.sentEvents().get(0);
        assertEquals(PlanActEventTypes.PLAN_READY, planReady.getType());

        PlanActAgent.advance(planReady, context);
        Event stepFinished = context.sentEvents().get(1);
        assertEquals(PlanActEventTypes.STEP_FINISHED, stepFinished.getType());

        PlanActAgent.advance(stepFinished, context);
        Event outputEvent = context.sentEvents().get(2);
        assertInstanceOf(OutputEvent.class, outputEvent);
        PlanActResult result = (PlanActResult) ((OutputEvent) outputEvent).getOutput();
        assertEquals("task-1", result.getTaskId());
        assertEquals("订单 ORD-1001 已发货。", result.getFinalAnswer());
        assertEquals(1, result.getPlan().size());
        assertTrue(result.getStepResults().get(0).getOutput().contains("status=SHIPPED"));
    }

    private static ResourceDescriptor testModel() {
        return ResourceDescriptor.Builder.newBuilder(OpenAICompletionsSetup.class.getName())
                .addInitialArgument("connection", "test-connection")
                .addInitialArgument("model", "test-model")
                .build();
    }
}