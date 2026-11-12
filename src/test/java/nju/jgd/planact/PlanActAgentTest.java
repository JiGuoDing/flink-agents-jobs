package nju.jgd.planact;

import nju.jgd.tool.support.OrderTools;

import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.integrations.chatmodels.openai.OpenAICompletionsSetup;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlanActAgentTest {

    @Test
    void wiresModelToolsAndActions() throws Exception {
        PlanActAgent agent =
                PlanActAgent.builder()
                        .chatModel(testModel())
                        .tool(tool())
                        .maxSteps(3)
                        .build();

        assertTrue(
                agent.getResources()
                        .get(ResourceType.CHAT_MODEL)
                        .containsKey(PlanActAgent.CHAT_MODEL_NAME));
        assertTrue(
                agent.getResources()
                        .get(ResourceType.TOOL)
                        .containsKey("lookupOrderStatus"));
        assertTrue(agent.getActions().containsKey("plan"));
        assertTrue(agent.getActions().containsKey("advance"));

        Map<String, Object> config = agent.getActions().get("plan").f2;
        assertEquals(3, config.get("maxSteps"));
        assertEquals(List.of("lookupOrderStatus"), config.get("allowedTools"));
        assertTrue(config.get("toolCatalog").toString().contains("lookupOrderStatus"));
    }

    @Test
    void rejectsMissingTools() {
        assertThrows(
                PlanActException.class,
                () -> PlanActAgent.builder().chatModel(testModel()).build());
    }

    @Test
    void rejectsInvalidMaxSteps() {
        assertThrows(
                PlanActException.class,
                () -> PlanActAgent.builder().chatModel(testModel()).tool(tool()).maxSteps(0));
        assertThrows(
                PlanActException.class,
                () -> PlanActAgent.builder().chatModel(testModel()).tool(tool()).maxSteps(21));
    }

    private static ResourceDescriptor testModel() {
        return ResourceDescriptor.Builder.newBuilder(OpenAICompletionsSetup.class.getName())
                .addInitialArgument("connection", "test-connection")
                .addInitialArgument("model", "test-model")
                .build();
    }

    private static PlanActTool tool() throws Exception {
        return PlanActTool.fromStaticMethod(
                OrderTools.class.getMethod("lookupOrderStatus", String.class));
    }
}