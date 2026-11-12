package nju.jgd.job.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * External runtime configuration for the customer-support demo job.
 *
 * <p>The configuration is loaded in the client process before the Flink job graph is built.
 * Credentials are intentionally kept outside the application JAR.
 */
public final class SupportJobConfig {

    public static final String DEFAULT_CONFIG_PATH = "config/support-agent.properties";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String baseUrl;
    private final String apiKey;
    private final String model;
    private final double temperature;
    private final int maxTokens;
    private final int timeoutSeconds;
    private final int maxRetries;
    private final Map<String, String> llmDefaultHeaders;
    private final Map<String, Object> llmAdditionalKwargs;
    private final int asyncThreads;
    private final int maxInFlightRecords;
    private final long memoryTtlSeconds;
    private final long cacheTtlSeconds;
    private final int planActMaxSteps;
    private final int parallelism;
    private final long checkpointIntervalMs;
    private final long checkpointMinPauseMs;
    private final long checkpointTimeoutMs;
    private final String checkpointDir;
    private final double demoRecordsPerSecond;

    private SupportJobConfig(
            String baseUrl,
            String apiKey,
            String model,
            double temperature,
            int maxTokens,
            int timeoutSeconds,
            int maxRetries,
            Map<String, String> llmDefaultHeaders,
            Map<String, Object> llmAdditionalKwargs,
            int asyncThreads,
            int maxInFlightRecords,
            long memoryTtlSeconds,
            long cacheTtlSeconds,
            int planActMaxSteps,
            int parallelism,
            long checkpointIntervalMs,
            long checkpointMinPauseMs,
            long checkpointTimeoutMs,
            String checkpointDir,
            double demoRecordsPerSecond) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.model = model;
        this.temperature = temperature;
        this.maxTokens = maxTokens;
        this.timeoutSeconds = timeoutSeconds;
        this.maxRetries = maxRetries;
        this.llmDefaultHeaders = llmDefaultHeaders == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(llmDefaultHeaders);
        this.llmAdditionalKwargs = llmAdditionalKwargs == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(llmAdditionalKwargs);
        this.asyncThreads = asyncThreads;
        this.maxInFlightRecords = maxInFlightRecords;
        this.memoryTtlSeconds = memoryTtlSeconds;
        this.cacheTtlSeconds = cacheTtlSeconds;
        this.planActMaxSteps = planActMaxSteps;
        this.parallelism = parallelism;
        this.checkpointIntervalMs = checkpointIntervalMs;
        this.checkpointMinPauseMs = checkpointMinPauseMs;
        this.checkpointTimeoutMs = checkpointTimeoutMs;
        this.checkpointDir = checkpointDir;
        this.demoRecordsPerSecond = demoRecordsPerSecond;
    }

    /**
     * Loads configuration from {@code --config <path>} or the default external path.
     *
     * @param args job arguments
     * @return validated configuration
     */
    public static SupportJobConfig load(String[] args) {
        Path path = resolveConfigPath(args);
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException(
                    "Configuration file not found: " + path.toAbsolutePath().normalize());
        }

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException exception) {
            throw new IllegalArgumentException(
                    "Unable to read configuration file: " + path.toAbsolutePath().normalize(),
                    exception);
        }
        return fromProperties(properties);
    }

    /** Visible for tests and alternative bootstrap code. */
    public static SupportJobConfig fromProperties(Properties properties) {
        String baseUrl = required(properties, "llm.base-url");
        String apiKey = required(properties, "llm.api-key");
        String model = valueOrDefault(properties, "llm.model", "qwen3-0.6b");

        double temperature = doubleValue(properties, "llm.temperature", 0.2d);
        int maxTokens = intValue(properties, "llm.max-tokens", 800);
        int timeoutSeconds = intValue(properties, "llm.timeout-seconds", 120);
        int maxRetries = intValue(properties, "llm.max-retries", 2);
        Map<String, String> llmDefaultHeaders =
                parseDefaultHeaders(properties.getProperty("llm.default-headers", ""));
        Map<String, Object> llmAdditionalKwargs =
                parseAdditionalKwargs(properties.getProperty("llm.additional-kwargs", ""));
        int asyncThreads = intValue(properties, "agent.async-threads", 2);
        int maxInFlightRecords = intValue(properties, "agent.max-in-flight-records", 1);
        long memoryTtlSeconds = longValue(properties, "agent.memory-ttl-seconds", 86_400L);
        long cacheTtlSeconds = longValue(properties, "cache.ttl-seconds", 600L);
        int planActMaxSteps = intValue(properties, "planact.max-steps", 8);
        int parallelism = intValue(properties, "job.parallelism", 1);
        long checkpointIntervalMs = longValue(properties, "job.checkpoint.interval-ms", 10_000L);
        long checkpointMinPauseMs = longValue(properties, "job.checkpoint.min-pause-ms", 5_000L);
        long checkpointTimeoutMs = longValue(properties, "job.checkpoint.timeout-ms", 60_000L);
        String checkpointDir = properties.getProperty("job.checkpoint.dir", "").trim();
        double demoRecordsPerSecond = doubleValue(properties, "demo.records-per-second", 1.0d);

        requireRange("llm.temperature", temperature, 0.0d, 2.0d);
        requirePositive("llm.max-tokens", maxTokens);
        requirePositive("llm.timeout-seconds", timeoutSeconds);
        requireNonNegative("llm.max-retries", maxRetries);
        requirePositive("agent.async-threads", asyncThreads);
        requirePositive("agent.max-in-flight-records", maxInFlightRecords);
        requirePositive("agent.memory-ttl-seconds", memoryTtlSeconds);
        requirePositive("cache.ttl-seconds", cacheTtlSeconds);
        requirePositive("planact.max-steps", planActMaxSteps);
        if (planActMaxSteps > 20) {
            throw new IllegalArgumentException("planact.max-steps must be at most 20");
        }
        requirePositive("job.parallelism", parallelism);
        requirePositive("job.checkpoint.interval-ms", checkpointIntervalMs);
        requireNonNegative("job.checkpoint.min-pause-ms", checkpointMinPauseMs);
        requirePositive("job.checkpoint.timeout-ms", checkpointTimeoutMs);
        requirePositive("demo.records-per-second", demoRecordsPerSecond);
        if (cacheTtlSeconds > memoryTtlSeconds) {
            throw new IllegalArgumentException(
                    "cache.ttl-seconds must be less than or equal to agent.memory-ttl-seconds");
        }

        return new SupportJobConfig(
                baseUrl,
                apiKey,
                model,
                temperature,
                maxTokens,
                timeoutSeconds,
                maxRetries,
                llmDefaultHeaders,
                llmAdditionalKwargs,
                asyncThreads,
                maxInFlightRecords,
                memoryTtlSeconds,
                cacheTtlSeconds,
                planActMaxSteps,
                parallelism,
                checkpointIntervalMs,
                checkpointMinPauseMs,
                checkpointTimeoutMs,
                checkpointDir,
                demoRecordsPerSecond);
    }

    private static Path resolveConfigPath(String[] args) {
        if (args == null || args.length == 0) {
            return Path.of(DEFAULT_CONFIG_PATH);
        }
        if (args.length == 2 && "--config".equals(args[0])) {
            return Path.of(args[1]);
        }
        if (args.length == 1 && args[0].startsWith("--config=")) {
            return Path.of(args[0].substring("--config=".length()));
        }
        throw new IllegalArgumentException(
                "Usage: CustomerSupportAgentJob [--config <path>]");
    }

    private static Map<String, String> parseDefaultHeaders(String raw) {
        if (raw == null || raw.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, String> headers =
                    MAPPER.readValue(raw, new TypeReference<Map<String, String>>() {});
            return headers == null ? Map.of() : Map.copyOf(headers);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(
                    "llm.default-headers must be a JSON object of string headers", exception);
        }
    }

    private static Map<String, Object> parseAdditionalKwargs(String raw) {
        if (raw == null || raw.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, Object> additionalKwargs =
                    MAPPER.readValue(raw, new TypeReference<Map<String, Object>>() {});
            return additionalKwargs == null ? Map.of() : new LinkedHashMap<>(additionalKwargs);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(
                    "llm.additional-kwargs must be a JSON object", exception);
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key, "").trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(key + " must not be blank");
        }
        return value;
    }

    private static String valueOrDefault(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key, defaultValue).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(key + " must not be blank");
        }
        return value;
    }

    private static int intValue(Properties properties, String key, int defaultValue) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid integer for " + key, exception);
        }
    }

    private static long longValue(Properties properties, String key, long defaultValue) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid long value for " + key, exception);
        }
    }

    private static double doubleValue(Properties properties, String key, double defaultValue) {
        String raw = properties.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid decimal value for " + key, exception);
        }
    }

    private static void requirePositive(String key, long value) {
        if (value <= 0L) {
            throw new IllegalArgumentException(key + " must be greater than zero");
        }
    }

    private static void requirePositive(String key, double value) {
        if (Double.isNaN(value) || value <= 0.0d) {
            throw new IllegalArgumentException(key + " must be greater than zero");
        }
    }

    private static void requireNonNegative(String key, long value) {
        if (value < 0L) {
            throw new IllegalArgumentException(key + " must not be negative");
        }
    }

    private static void requireRange(String key, double value, double minimum, double maximum) {
        if (Double.isNaN(value) || value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    key + " must be between " + minimum + " and " + maximum);
        }
    }

    public String baseUrl() {
        return baseUrl;
    }

    public String apiKey() {
        return apiKey;
    }

    public String model() {
        return model;
    }

    public double temperature() {
        return temperature;
    }

    public int maxTokens() {
        return maxTokens;
    }

    public int timeoutSeconds() {
        return timeoutSeconds;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public Map<String, String> llmDefaultHeaders() {
        return Map.copyOf(llmDefaultHeaders);
    }

    public Map<String, Object> llmAdditionalKwargs() {
        return Map.copyOf(llmAdditionalKwargs);
    }

    public int asyncThreads() {
        return asyncThreads;
    }

    public int maxInFlightRecords() {
        return maxInFlightRecords;
    }

    public long memoryTtlSeconds() {
        return memoryTtlSeconds;
    }

    public long cacheTtlSeconds() {
        return cacheTtlSeconds;
    }

    public int planActMaxSteps() {
        return planActMaxSteps;
    }

    public int parallelism() {
        return parallelism;
    }

    public long checkpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public long checkpointMinPauseMs() {
        return checkpointMinPauseMs;
    }

    public long checkpointTimeoutMs() {
        return checkpointTimeoutMs;
    }

    public String checkpointDir() {
        return checkpointDir;
    }

    public double demoRecordsPerSecond() {
        return demoRecordsPerSecond;
    }
}

