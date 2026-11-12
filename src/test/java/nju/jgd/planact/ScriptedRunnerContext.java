package nju.jgd.planact;

import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.ChatResult;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.context.AsyncFuture;
import org.apache.flink.agents.api.context.DurableCallable;
import org.apache.flink.agents.api.context.DurableFuture;
import org.apache.flink.agents.api.context.MemoryObject;
import org.apache.flink.agents.api.context.Outcome;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.configuration.ReadableConfiguration;
import org.apache.flink.agents.api.event.ChatRequestEvent;
import org.apache.flink.agents.api.memory.BaseLongTermMemory;
import org.apache.flink.agents.api.metrics.FlinkAgentsMetricGroup;
import org.apache.flink.agents.api.resource.Resource;
import org.apache.flink.agents.api.resource.ResourceType;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/** Deterministic RunnerContext used to exercise PlanActAgent without a live model. */
final class ScriptedRunnerContext implements RunnerContext {

    private final TestMemoryObject sensoryMemory = new TestMemoryObject();
    private final Map<String, Object> actionConfig;
    private final Map<String, Resource> resources;
    private final Deque<ChatResult> chatResults;
    private final List<Event> sentEvents = new java.util.ArrayList<>();

    ScriptedRunnerContext(
            Map<String, Object> actionConfig,
            Map<String, Resource> resources,
            List<String> chatAnswers) {
        this.actionConfig = Map.copyOf(actionConfig);
        this.resources = Map.copyOf(resources);
        this.chatResults = new ArrayDeque<>();
        for (String answer : chatAnswers) {
            chatResults.add(new ChatResult(new ChatMessage(MessageRole.ASSISTANT, answer)));
        }
    }

    List<Event> sentEvents() {
        return sentEvents;
    }

    @Override
    public void sendEvent(Event event) {
        sentEvents.add(event);
    }

    @Override
    public MemoryObject getSensoryMemory() {
        return sensoryMemory;
    }

    @Override
    public MemoryObject getShortTermMemory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BaseLongTermMemory getLongTermMemory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FlinkAgentsMetricGroup getAgentMetricGroup() {
        return null;
    }

    @Override
    public FlinkAgentsMetricGroup getActionMetricGroup() {
        return null;
    }

    @Override
    public Resource getResource(String name, ResourceType type) {
        Resource resource = resources.get(name);
        if (resource == null) {
            throw new IllegalArgumentException("Missing test resource: " + name);
        }
        return resource;
    }

    @Override
    public ReadableConfiguration getConfig() {
        return null;
    }

    @Override
    public Map<String, Object> getActionConfig() {
        return actionConfig;
    }

    @Override
    public Object getActionConfigValue(String key) {
        return actionConfig.get(key);
    }

    @Override
    public <T> AsyncFuture<T> executeAsync(Callable<T> callable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T durableExecute(DurableCallable<T> callable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> DurableFuture<T> durableExecuteAsync(DurableCallable<T> callable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> AsyncFuture<List<Outcome<T>>> gather(List<? extends AsyncFuture<T>> futures) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DurableFuture<ChatResult> chat(ChatRequestEvent request) {
        return new DurableFuture<>() {
            @Override
            public ChatResult await() {
                ChatResult result = chatResults.pollFirst();
                if (result == null) {
                    throw new IllegalStateException("No scripted chat result remains");
                }
                return result;
            }
        };
    }

    @Override
    public void close() {}
}