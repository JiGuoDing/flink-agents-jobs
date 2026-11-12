package nju.jgd.job;

import nju.jgd.job.config.SupportJobConfig;
import nju.jgd.model.SupportRequest;
import nju.jgd.model.SupportResponse;
import nju.jgd.operator.agent.CustomerSupportAgent;
import nju.jgd.operator.source.SupportRequestSource;
import nju.jgd.operator.state.UserSessionStateFunction;
import org.apache.flink.agents.api.AgentsExecutionEnvironment;
import org.apache.flink.agents.api.agents.AgentExecutionOptions;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.integrations.chatmodels.openai.VLLMChatModelConnection;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * Complete customer-support Flink Agent job with keyed state, real inference, tools, memory, and an
 * exact-match Flink-state cache.
 */
public final class CustomerSupportAgentJob {

    private static final String JOB_NAME = "Customer Support Agent Job";

    private CustomerSupportAgentJob() {}

    public static void main(String[] args) throws Exception {
        SupportJobConfig config = SupportJobConfig.load(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(config.parallelism());
        configureCheckpointing(env, config);

        AgentsExecutionEnvironment agentsEnv =
                AgentsExecutionEnvironment.getExecutionEnvironment(env);
        agentsEnv.getConfig().set(AgentExecutionOptions.NUM_ASYNC_THREADS, config.asyncThreads());
        agentsEnv
                .getConfig()
                .set(
                        AgentExecutionOptions.MAX_IN_FLIGHT_INPUT_RECORDS,
                        config.maxInFlightRecords());
        agentsEnv
                .getConfig()
                .set(
                        AgentExecutionOptions.SHORT_TERM_MEMORY_STATE_TTL_MS,
                        Math.multiplyExact(config.memoryTtlSeconds(), 1_000L));

        agentsEnv.addResource(
                CustomerSupportAgent.CONNECTION_NAME,
                ResourceType.CHAT_MODEL_CONNECTION,
                openAIConnection(config));

        DataStream<SupportRequest> source =
                env.fromSource(
                        SupportRequestSource.create(config.demoRecordsPerSecond()),
                        WatermarkStrategy.noWatermarks(),
                        SupportRequestSource.SOURCE_NAME);

        SingleOutputStreamOperator<SupportRequest> enriched =
                source.keyBy((KeySelector<SupportRequest, String>) SupportRequest::getUserId)
                        .process(new UserSessionStateFunction(config.memoryTtlSeconds()))
                        .name("user-session-state")
                        .uid("user-session-state");

        CustomerSupportAgent agent =
                new CustomerSupportAgent(
                        config.model(),
                        config.temperature(),
                        config.maxTokens(),
                        config.cacheTtlSeconds(),
                        config.llmAdditionalKwargs());

        DataStream<Object> agentOutput =
                agentsEnv
                        .fromDataStream(
                                enriched,
                                (KeySelector<SupportRequest, String>) SupportRequest::getUserId)
                        .apply(agent)
                        .toDataStream();

        DataStream<SupportResponse> responses =
                agentOutput
                        .map(value -> (SupportResponse) value)
                        .returns(SupportResponse.class)
                        .name("normalize-support-response")
                        .uid("normalize-support-response");

        responses.print("support-response").uid("support-response-print");
        agentsEnv.execute(JOB_NAME);
    }

    private static ResourceDescriptor openAIConnection(SupportJobConfig config) {
        ResourceDescriptor.Builder builder =
                ResourceDescriptor.Builder.newBuilder(VLLMChatModelConnection.class.getName())
                        .addInitialArgument("api_key", config.apiKey())
                        .addInitialArgument("api_base_url", config.baseUrl())
                        .addInitialArgument("timeout", config.timeoutSeconds())
                        .addInitialArgument("max_retries", config.maxRetries())
                        .addInitialArgument("model", config.model());
        Map<String, String> defaultHeaders = config.llmDefaultHeaders();
        if (!defaultHeaders.isEmpty()) {
            builder.addInitialArgument("default_headers", defaultHeaders);
        }
        return builder.build();
    }

    private static void configureCheckpointing(
            StreamExecutionEnvironment env, SupportJobConfig config) {
        env.enableCheckpointing(config.checkpointIntervalMs());
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(config.checkpointMinPauseMs());
        checkpointConfig.setCheckpointTimeout(config.checkpointTimeoutMs());
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        if (config.checkpointDir() != null && !config.checkpointDir().isBlank()) {
            Configuration flinkConfiguration = new Configuration();
            flinkConfiguration.set(
                    CheckpointingOptions.CHECKPOINT_STORAGE, config.checkpointDir());
            env.configure(flinkConfiguration);
        }
    }
}


