package nju.jgd.operator.agent;

import org.apache.flink.agents.api.resource.ResourceType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomerSupportAgentTest {

    @Test
    void wiresModelPromptToolsAndActions() throws Exception {
        CustomerSupportAgent agent = new CustomerSupportAgent("gpt-4o-mini", 0.2d, 800, 600L);

        assertTrue(agent.getResources().get(ResourceType.CHAT_MODEL).containsKey(CustomerSupportAgent.CHAT_MODEL_NAME));
        assertTrue(agent.getResources().get(ResourceType.PROMPT).containsKey(CustomerSupportAgent.PROMPT_NAME));
        assertTrue(agent.getResources().get(ResourceType.TOOL).containsKey(CustomerSupportAgent.TOOL_LOOKUP_ORDER));
        assertTrue(agent.getResources().get(ResourceType.TOOL).containsKey(CustomerSupportAgent.TOOL_CHECK_REFUND));
        assertTrue(agent.getActions().containsKey("handleInput"));
        assertTrue(agent.getActions().containsKey("handleChatResponse"));
    }
}
