package nju.jgd.planact;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.agents.Agent;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.ChatResult;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.context.MemoryObject;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.event.ChatRequestEvent;
import org.apache.flink.agents.api.function.JavaFunction;
import org.apache.flink.agents.api.resource.Resource;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.agents.api.tools.ToolParameters;
import org.apache.flink.agents.api.tools.ToolResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reusable fixed-plan Plan-Act agent.
 *
 * <p>The planner produces a complete tool plan, the agent executes exactly one tool per internal
 * event, and a final chat call synthesizes the answer. The plan and execution state live in
 * sensory memory so they are scoped to one Flink Agents run and checkpointed by the runtime.
 */
public final class PlanActAgent extends Agent {

    public static final String CHAT_MODEL_NAME = "planActChatModel";
    public static final int DEFAULT_MAX_STEPS = 8;
    public static final int HARD_MAX_STEPS = 20;

    private static final Logger LOG = LoggerFactory.getLogger(PlanActAgent.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final PlanActEngine ENGINE = new PlanActEngine();

    private static final String ACTION_MODEL = "model";
    private static final String ACTION_MAX_STEPS = "maxSteps";
    private static final String ACTION_ALLOWED_TOOLS = "allowedTools";
    private static final String ACTION_TOOL_CATALOG = "toolCatalog";
    private static final String ACTION_PLANNER_INSTRUCTIONS = "plannerInstructions";
    private static final String ACTION_SYNTHESIS_INSTRUCTIONS = "synthesisInstructions";

    private static final String DEFAULT_PLANNER_INSTRUCTIONS =
            """
            You are a Plan-Act planner. Create the smallest complete sequence of tool calls
            needed to achieve the user's goal. Use only the tools listed by the user message.
            Do not execute tools yourself. Do not invent tool names or arguments.
            The final answer is produced later by a separate synthesis step, so do not add
            an answer step to the plan.
            """;

    private static final String DEFAULT_SYNTHESIS_INSTRUCTIONS =
            """
            You are the final answer writer of a Plan-Act agent. Use only the supplied goal,
            context, plan, and tool results. Do not invent facts. If a tool reports that data
            was not found, state that clearly. Answer the user's goal concisely.
            """;

    private PlanActAgent(
            ResourceDescriptor chatModel,
            Map<String, PlanActTool> tools,
            int maxSteps,
            String plannerInstructions,
            String synthesisInstructions)
            throws Exception {
        addResource(CHAT_MODEL_NAME, ResourceType.CHAT_MODEL, chatModel);
        for (Map.Entry<String, PlanActTool> entry : tools.entrySet()) {
            addResource(entry.getKey(), ResourceType.TOOL, entry.getValue().functionTool());
        }

        Map<String, Object> actionConfig = new LinkedHashMap<>();
        actionConfig.put(ACTION_MODEL, CHAT_MODEL_NAME);
        actionConfig.put(ACTION_MAX_STEPS, maxSteps);
        actionConfig.put(ACTION_ALLOWED_TOOLS, List.copyOf(tools.keySet()));
        actionConfig.put(ACTION_TOOL_CATALOG, buildToolCatalog(tools));
        actionConfig.put(ACTION_PLANNER_INSTRUCTIONS, plannerInstructions);
        actionConfig.put(ACTION_SYNTHESIS_INSTRUCTIONS, synthesisInstructions);

        addAction(
                "plan",
                new String[] {InputEvent.EVENT_TYPE},
                JavaFunction.fromMethod(
                        PlanActAgent.class.getMethod(
                                "plan", InputEvent.class, RunnerContext.class)),
                actionConfig);
        addAction(
                "advance",
                new String[] {PlanActEventTypes.PLAN_READY, PlanActEventTypes.STEP_FINISHED},
                JavaFunction.fromMethod(
                        PlanActAgent.class.getMethod(
                                "advance", Event.class, RunnerContext.class)),
                actionConfig);
    }

    public static Builder builder() {
        return new Builder();
    }
    /** Planner action: create and persist the complete plan. */
    public static void plan(InputEvent event, RunnerContext context) throws Exception {
        Object inputObject = event.getInput();
        if (!(inputObject instanceof PlanActInput input)) {
            throw new PlanActException(
                    "PlanActAgent expects PlanActInput but received "
                            + (inputObject == null ? "null" : inputObject.getClass().getName()));
        }
        input.validate();

        Settings settings = Settings.from(context);
        MemoryObject sensoryMemory = context.getSensoryMemory();
        if (PlanActStateStore.hasState(sensoryMemory)) {
            PlanActExecution existing = PlanActStateStore.load(sensoryMemory);
            if (existing.getInput().getTaskId().equals(input.getTaskId()) && !existing.isComplete()) {
                LOG.info(
                        "Resuming Plan-Act task {} at step {}",
                        input.getTaskId(),
                        existing.getNextStepIndex());
                context.sendEvent(
                        PlanActEventTypes.planReady(
                                input.getTaskId(), existing.getNextStepIndex()));
                return;
            }
            throw new PlanActException(
                    "An active Plan-Act execution already exists for this key; taskId="
                            + existing.getInput().getTaskId());
        }

        String userPrompt =
                """
                Goal:
                %s

                Context (JSON):
                %s

                Available tools:
                %s

                Return exactly one JSON object, without markdown, in this shape:
                {
                  "steps": [
                    {
                      "id": "step-1",
                      "tool": "exact_tool_name",
                      "arguments": {"argumentName": "argumentValue"},
                      "purpose": "why this step is needed"
                    }
                  ]
                }

                The steps array must contain between 1 and %d entries. Step ids must be unique.
                Only use tools listed above. Do not add a final-answer step.
                """
                        .formatted(
                                input.getGoal(),
                                toJson(input.getContext()),
                                settings.toolCatalog(),
                                settings.maxSteps());

        ChatRequestEvent request =
                new ChatRequestEvent(
                        settings.model(),
                        List.of(
                                new ChatMessage(
                                        MessageRole.SYSTEM, settings.plannerInstructions()),
                                new ChatMessage(MessageRole.USER, userPrompt)));
        ChatResult result = context.chat(request).await();
        String planText = result == null ? null : result.getText();
        PlanActPlan plan =
                new PlanActPlanParser().parse(planText, settings.allowedTools(), settings.maxSteps());
        PlanActExecution execution = ENGINE.start(input, plan, settings.maxSteps());
        PlanActStateStore.save(sensoryMemory, execution);

        LOG.info(
                "Created Plan-Act plan for task {} with {} steps: {}",
                input.getTaskId(),
                execution.getPlan().size(),
                execution.getPlan().stream().map(PlanActStep::getTool).toList());
        context.sendEvent(PlanActEventTypes.planReady(input.getTaskId(), 0));
    }

    /** Advance exactly one step, or synthesize the final answer once all steps are complete. */
    public static void advance(Event event, RunnerContext context) throws Exception {
        Settings settings = Settings.from(context);
        MemoryObject sensoryMemory = context.getSensoryMemory();
        PlanActExecution execution = PlanActStateStore.load(sensoryMemory);
        String taskId = PlanActEventTypes.taskId(event);
        int eventStepIndex = PlanActEventTypes.stepIndex(event);

        if (!execution.getInput().getTaskId().equals(taskId)) {
            throw new PlanActException(
                    "Plan-Act event taskId '" + taskId + "' does not match active task '"
                            + execution.getInput().getTaskId() + "'");
        }
        if (eventStepIndex < execution.getNextStepIndex()) {
            LOG.debug(
                    "Ignoring stale Plan-Act event for task {} at step {}; next step is {}",
                    taskId,
                    eventStepIndex,
                    execution.getNextStepIndex());
            return;
        }
        if (eventStepIndex > execution.getNextStepIndex()) {
            throw new PlanActException(
                    "Plan-Act event jumped to step " + eventStepIndex + " but next step is "
                            + execution.getNextStepIndex());
        }

        if (execution.isComplete()) {
            synthesize(execution, settings, context);
            return;
        }

        PlanActStep step = ENGINE.currentStep(execution);
        Resource resource = context.getResource(step.getTool(), ResourceType.TOOL);
        if (!(resource instanceof Tool tool)) {
            throw new PlanActException(
                    "Plan-Act tool '" + step.getTool() + "' is not a Tool resource");
        }

        long startedAt = System.currentTimeMillis();
        ToolResponse response;
        try {
            response = tool.call(new ToolParameters(step.getArguments()));
        } catch (RuntimeException exception) {
            throw new PlanActException(
                    "Plan-Act tool '" + step.getTool() + "' failed at step '" + step.getId() + "'",
                    exception);
        }
        long durationMs = Math.max(0L, System.currentTimeMillis() - startedAt);
        if (response == null || response.isError()) {
            String error = response == null ? "no response" : response.getError();
            throw new PlanActException(
                    "Plan-Act tool '" + step.getTool() + "' returned an error at step '"
                            + step.getId() + "': " + error);
        }

        PlanActStepResult stepResult =
                new PlanActStepResult(step.getId(), step.getTool(), response.getText(), durationMs);
        PlanActExecution nextExecution = ENGINE.recordStepResult(execution, stepResult);
        PlanActStateStore.save(sensoryMemory, nextExecution);

        LOG.info(
                "Completed Plan-Act task {} step {}/{} tool={} durationMs={}",
                taskId,
                nextExecution.getNextStepIndex(),
                nextExecution.getPlan().size(),
                step.getTool(),
                durationMs);
        context.sendEvent(
                PlanActEventTypes.stepFinished(taskId, nextExecution.getNextStepIndex()));
    }
    private static void synthesize(
            PlanActExecution execution,
            Settings settings,
            RunnerContext context)
            throws Exception {
        String prompt =
                """
                Goal:
                %s

                Context (JSON):
                %s

                Executed plan (JSON):
                %s

                Tool results (JSON):
                %s

                Write the final answer for the user.
                """
                        .formatted(
                                execution.getInput().getGoal(),
                                toJson(execution.getInput().getContext()),
                                toJson(execution.getPlan()),
                                toJson(execution.getResults()));
        ChatRequestEvent request =
                new ChatRequestEvent(
                        settings.model(),
                        List.of(
                                new ChatMessage(
                                        MessageRole.SYSTEM, settings.synthesisInstructions()),
                                new ChatMessage(MessageRole.USER, prompt)));
        ChatResult result = context.chat(request).await();
        String answer = result == null ? null : result.getText();
        if (answer == null || answer.isBlank()) {
            throw new PlanActException(
                    "Plan-Act final synthesis returned an empty answer for task '"
                            + execution.getInput().getTaskId() + "'");
        }
        String model =
                result.getModel() == null || result.getModel().isBlank()
                        ? settings.model()
                        : result.getModel();
        PlanActResult output = ENGINE.finish(execution, answer, model);
        LOG.info(
                "Finished Plan-Act task {} steps={} model={} totalDurationMs={}",
                output.getTaskId(),
                output.getPlan().size(),
                output.getModel(),
                output.getTotalDurationMs());
        context.sendEvent(new OutputEvent(output));
    }

    private static String buildToolCatalog(Map<String, PlanActTool> tools) {
        StringBuilder catalog = new StringBuilder();
        for (Map.Entry<String, PlanActTool> entry : tools.entrySet()) {
            PlanActTool tool = entry.getValue();
            catalog.append("- ").append(entry.getKey()).append('\n');
            String description = tool.getDescription();
            if (description != null && !description.isBlank()) {
                catalog.append("  description: ").append(description).append('\n');
            }
            String schema = tool.getMetadata().getInputSchema();
            if (schema != null && !schema.isBlank()) {
                catalog.append("  input_schema: ").append(schema).append('\n');
            }
        }
        return catalog.toString();
    }

    private static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new PlanActException("Unable to serialize Plan-Act prompt data", exception);
        }
    }

    private static final class Settings {
        private final String model;
        private final int maxSteps;
        private final Set<String> allowedTools;
        private final String toolCatalog;
        private final String plannerInstructions;
        private final String synthesisInstructions;

        private Settings(
                String model,
                int maxSteps,
                Set<String> allowedTools,
                String toolCatalog,
                String plannerInstructions,
                String synthesisInstructions) {
            this.model = model;
            this.maxSteps = maxSteps;
            this.allowedTools = allowedTools;
            this.toolCatalog = toolCatalog;
            this.plannerInstructions = plannerInstructions;
            this.synthesisInstructions = synthesisInstructions;
        }

        private static Settings from(RunnerContext context) {
            String model = stringConfig(context, ACTION_MODEL);
            int maxSteps = intConfig(context, ACTION_MAX_STEPS);
            if (maxSteps <= 0 || maxSteps > HARD_MAX_STEPS) {
                throw new PlanActException(
                        "maxSteps must be between 1 and " + HARD_MAX_STEPS);
            }
            Object rawTools = context.getActionConfigValue(ACTION_ALLOWED_TOOLS);
            if (!(rawTools instanceof List<?> toolList) || toolList.isEmpty()) {
                throw new PlanActException("PlanActAgent has no allowed tools");
            }
            Set<String> allowedTools = new LinkedHashSet<>();
            for (Object tool : toolList) {
                if (!(tool instanceof String name) || name.isBlank()) {
                    throw new PlanActException("PlanActAgent allowed tool names must be strings");
                }
                allowedTools.add(name);
            }
            return new Settings(
                    model,
                    maxSteps,
                    Set.copyOf(allowedTools),
                    stringConfig(context, ACTION_TOOL_CATALOG),
                    stringConfig(context, ACTION_PLANNER_INSTRUCTIONS),
                    stringConfig(context, ACTION_SYNTHESIS_INSTRUCTIONS));
        }

        private String model() {
            return model;
        }

        private int maxSteps() {
            return maxSteps;
        }

        private Set<String> allowedTools() {
            return allowedTools;
        }

        private String toolCatalog() {
            return toolCatalog;
        }

        private String plannerInstructions() {
            return plannerInstructions;
        }

        private String synthesisInstructions() {
            return synthesisInstructions;
        }
    }

    private static String stringConfig(RunnerContext context, String key) {
        Object value = context.getActionConfigValue(key);
        if (!(value instanceof String stringValue) || stringValue.isBlank()) {
            throw new PlanActException("Missing PlanActAgent action config: " + key);
        }
        return stringValue;
    }

    private static int intConfig(RunnerContext context, String key) {
        Object value = context.getActionConfigValue(key);
        if (!(value instanceof Number number)) {
            throw new PlanActException("PlanActAgent action config must be numeric: " + key);
        }
        return number.intValue();
    }

    /** Builder for the reusable Plan-Act agent. */
    public static final class Builder {
        private ResourceDescriptor chatModel;
        private final Map<String, PlanActTool> tools = new LinkedHashMap<>();
        private int maxSteps = DEFAULT_MAX_STEPS;
        private String plannerInstructions = DEFAULT_PLANNER_INSTRUCTIONS;
        private String synthesisInstructions = DEFAULT_SYNTHESIS_INSTRUCTIONS;

        public Builder chatModel(ResourceDescriptor descriptor) {
            this.chatModel = descriptor;
            return this;
        }

        public Builder tool(PlanActTool tool) {
            if (tool == null) {
                throw new PlanActException("PlanActAgent tool must not be null");
            }
            String name = tool.getMetadata().getName();
            if (name == null || name.isBlank()) {
                throw new PlanActException("PlanActAgent tool metadata must contain a name");
            }
            if (tools.putIfAbsent(name, tool) != null) {
                throw new PlanActException("Duplicate PlanActAgent tool: " + name);
            }
            return this;
        }

        public Builder maxSteps(int maxSteps) {
            if (maxSteps <= 0 || maxSteps > HARD_MAX_STEPS) {
                throw new PlanActException(
                        "maxSteps must be between 1 and " + HARD_MAX_STEPS);
            }
            this.maxSteps = maxSteps;
            return this;
        }

        public Builder plannerInstructions(String instructions) {
            if (instructions == null || instructions.isBlank()) {
                throw new PlanActException("plannerInstructions must not be blank");
            }
            this.plannerInstructions = instructions;
            return this;
        }

        public Builder synthesisInstructions(String instructions) {
            if (instructions == null || instructions.isBlank()) {
                throw new PlanActException("synthesisInstructions must not be blank");
            }
            this.synthesisInstructions = instructions;
            return this;
        }

        public PlanActAgent build() throws Exception {
            if (chatModel == null) {
                throw new PlanActException("PlanActAgent chatModel must be configured");
            }
            if (tools.isEmpty()) {
                throw new PlanActException("PlanActAgent requires at least one tool");
            }
            return new PlanActAgent(
                    chatModel,
                    new LinkedHashMap<>(tools),
                    maxSteps,
                    plannerInstructions,
                    synthesisInstructions);
        }
    }
}