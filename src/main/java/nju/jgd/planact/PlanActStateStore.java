package nju.jgd.planact;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.agents.api.context.MemoryObject;

/** Persists one Plan-Act execution in Flink Agents sensory memory. */
public final class PlanActStateStore {

    public static final String STATE_PATH = "planact.execution";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PlanActStateStore() {}

    public static boolean hasState(MemoryObject memory) throws Exception {
        if (memory == null) {
            throw new PlanActException("Sensory memory must not be null");
        }
        if (!memory.isExist(STATE_PATH)) {
            return false;
        }
        Object value = memory.get(STATE_PATH).getValue();
        return value instanceof String stringValue && !stringValue.isBlank();
    }

    public static void save(MemoryObject memory, PlanActExecution execution) throws Exception {
        if (memory == null) {
            throw new PlanActException("Sensory memory must not be null");
        }
        if (execution == null) {
            throw new PlanActException("Plan-Act execution must not be null");
        }
        try {
            memory.set(STATE_PATH, MAPPER.writeValueAsString(execution));
        } catch (JsonProcessingException exception) {
            throw new PlanActException("Unable to serialize Plan-Act execution state", exception);
        }
    }

    public static PlanActExecution load(MemoryObject memory) throws Exception {
        if (!hasState(memory)) {
            throw new PlanActException("No active Plan-Act execution state was found");
        }
        Object value = memory.get(STATE_PATH).getValue();
        try {
            PlanActExecution execution = MAPPER.readValue((String) value, PlanActExecution.class);
            if (execution == null || execution.getInput() == null) {
                throw new PlanActException("Plan-Act execution state is incomplete");
            }
            execution.getInput().validate();
            if (execution.getPlan().isEmpty()) {
                throw new PlanActException("Plan-Act execution state has an empty plan");
            }
            return execution;
        } catch (JsonProcessingException exception) {
            throw new PlanActException("Unable to deserialize Plan-Act execution state", exception);
        }
    }
}