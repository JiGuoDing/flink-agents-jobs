package nju.jgd.planact;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlanActPlanParserTest {

    private static final Set<String> TOOLS =
            Set.of("lookupOrderStatus", "checkRefundEligibility");

    private final PlanActPlanParser parser = new PlanActPlanParser();

    @Test
    void parsesPlainJsonPlan() {
        PlanActPlan plan =
                parser.parse(
                        """
                        {
                          "steps": [
                            {
                              "id": "step-1",
                              "tool": "lookupOrderStatus",
                              "arguments": {"orderId": "ORD-1001"},
                              "purpose": "确认物流状态"
                            }
                          ]
                        }
                        """,
                        TOOLS,
                        4);

        assertEquals(1, plan.getSteps().size());
        assertEquals("step-1", plan.getSteps().get(0).getId());
        assertEquals("ORD-1001", plan.getSteps().get(0).getArguments().get("orderId"));
        assertEquals("确认物流状态", plan.getSteps().get(0).getPurpose());
    }

    @Test
    void acceptsMarkdownFenceAroundJson() {
        PlanActPlan plan =
                parser.parse(
                        """
                        ```json
                        {"steps":[{"id":"s1","tool":"lookupOrderStatus","arguments":{"orderId":"ORD-1002"}}]}
                        ```
                        """,
                        TOOLS,
                        2);

        assertEquals("lookupOrderStatus", plan.getSteps().get(0).getTool());
    }

    @Test
    void rejectsEmptyPlan() {
        assertThrows(
                PlanActException.class,
                () -> parser.parse("{\"steps\":[]}", TOOLS, 2));
    }

    @Test
    void rejectsInvalidJson() {
        assertThrows(
                PlanActException.class,
                () -> parser.parse("not-json", TOOLS, 2));
    }

    @Test
    void rejectsDuplicateStepIds() {
        assertThrows(
                PlanActException.class,
                () ->
                        parser.parse(
                                """
                                {"steps":[
                                  {"id":"same","tool":"lookupOrderStatus","arguments":{}},
                                  {"id":"same","tool":"lookupOrderStatus","arguments":{}}
                                ]}
                                """,
                                TOOLS,
                                2));
    }

    @Test
    void rejectsUnknownTool() {
        assertThrows(
                PlanActException.class,
                () ->
                        parser.parse(
                                """
                                {"steps":[{"id":"s1","tool":"deleteOrder","arguments":{}}]}
                                """,
                                TOOLS,
                                2));
    }

    @Test
    void rejectsTooManySteps() {
        assertThrows(
                PlanActException.class,
                () ->
                        parser.parse(
                                """
                                {"steps":[
                                  {"id":"s1","tool":"lookupOrderStatus","arguments":{}},
                                  {"id":"s2","tool":"lookupOrderStatus","arguments":{}}
                                ]}
                                """,
                                TOOLS,
                                1));
    }

    @Test
    void rejectsNonObjectArguments() {
        assertThrows(
                PlanActException.class,
                () ->
                        parser.parse(
                                """
                                {"steps":[{"id":"s1","tool":"lookupOrderStatus","arguments":["bad"]}]}
                                """,
                                TOOLS,
                                2));
    }

    @Test
    void rejectsBlankStepId() {
        assertThrows(
                PlanActException.class,
                () ->
                        parser.parse(
                                """
                                {"steps":[{"id":"","tool":"lookupOrderStatus","arguments":{}}]}
                                """,
                                TOOLS,
                                2));
    }
}