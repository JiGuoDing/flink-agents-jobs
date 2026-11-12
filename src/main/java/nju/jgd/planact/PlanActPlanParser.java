package nju.jgd.planact;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Parses the strict JSON plan contract returned by the planner model. */
public final class PlanActPlanParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> ARGUMENT_TYPE =
            new TypeReference<>() {};

    public PlanActPlan parse(String raw, Set<String> allowedTools, int maxSteps) {
        if (raw == null || raw.isBlank()) {
            throw new PlanActException("Planner returned an empty plan");
        }
        if (allowedTools == null || allowedTools.isEmpty()) {
            throw new PlanActException("No tools are registered for Plan-Act");
        }
        if (maxSteps <= 0) {
            throw new PlanActException("maxSteps must be greater than zero");
        }

        String json = extractJson(raw);
        JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (JsonProcessingException exception) {
            throw new PlanActException("Planner returned invalid JSON: " + exception.getOriginalMessage(), exception);
        }
        if (root == null || !root.isObject()) {
            throw new PlanActException("Planner response must be a JSON object");
        }

        JsonNode stepsNode = root.get("steps");
        if (stepsNode == null || !stepsNode.isArray() || stepsNode.isEmpty()) {
            throw new PlanActException("Planner response must contain a non-empty 'steps' array");
        }
        if (stepsNode.size() > maxSteps) {
            throw new PlanActException(
                    "Planner returned " + stepsNode.size() + " steps, exceeding maxSteps=" + maxSteps);
        }

        Set<String> stepIds = new HashSet<>();
        List<PlanActStep> steps = new ArrayList<>();
        for (int index = 0; index < stepsNode.size(); index++) {
            JsonNode stepNode = stepsNode.get(index);
            if (stepNode == null || !stepNode.isObject()) {
                throw new PlanActException("Plan step " + (index + 1) + " must be a JSON object");
            }

            String id = requiredText(stepNode, "id", index);
            String tool = requiredText(stepNode, "tool", index);
            if (!allowedTools.contains(tool)) {
                throw new PlanActException(
                        "Unknown Plan-Act tool '" + tool + "'. Allowed tools: " + allowedTools);
            }
            if (!stepIds.add(id)) {
                throw new PlanActException("Duplicate Plan-Act step id: " + id);
            }

            JsonNode argumentsNode = stepNode.get("arguments");
            Map<String, Object> arguments;
            if (argumentsNode == null || argumentsNode.isNull()) {
                arguments = Collections.emptyMap();
            } else if (!argumentsNode.isObject()) {
                throw new PlanActException(
                        "Plan step '" + id + "' arguments must be a JSON object");
            } else {
                arguments = MAPPER.convertValue(argumentsNode, ARGUMENT_TYPE);
            }

            String purpose = "";
            JsonNode purposeNode = stepNode.get("purpose");
            if (purposeNode != null && !purposeNode.isNull()) {
                if (!purposeNode.isTextual()) {
                    throw new PlanActException(
                            "Plan step '" + id + "' purpose must be a string");
                }
                purpose = purposeNode.asText();
            }
            PlanActStep step = new PlanActStep(id, tool, arguments, purpose);
            step.validate();
            steps.add(step);
        }
        return new PlanActPlan(steps);
    }

    private static String requiredText(JsonNode stepNode, String field, int index) {
        JsonNode value = stepNode.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            throw new PlanActException(
                    "Plan step " + (index + 1) + " must contain a non-blank '" + field + "' string");
        }
        return value.asText();
    }

    private static String extractJson(String raw) {
        String value = raw.trim();
        if (value.startsWith("```")) {
            int firstNewline = value.indexOf('\n');
            if (firstNewline >= 0) {
                value = value.substring(firstNewline + 1);
            }
            int closingFence = value.lastIndexOf("```");
            if (closingFence >= 0) {
                value = value.substring(0, closingFence);
            }
        }
        int firstBrace = value.indexOf('{');
        int lastBrace = value.lastIndexOf('}');
        if (firstBrace < 0 || lastBrace <= firstBrace) {
            throw new PlanActException("Planner response does not contain a JSON object");
        }
        return value.substring(firstBrace, lastBrace + 1);
    }
}