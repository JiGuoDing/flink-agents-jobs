package nju.jgd.job.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SupportJobConfigTest {

    @Test
    void loadsValidProperties() {
        SupportJobConfig config = SupportJobConfig.fromProperties(validProperties());

        assertEquals("https://model.example/v1", config.baseUrl());
        assertEquals("test-key", config.apiKey());
        assertEquals("gpt-4o-mini", config.model());
        assertEquals(0.2d, config.temperature());
        assertEquals(800, config.maxTokens());
        assertEquals(600L, config.cacheTtlSeconds());
        assertEquals(8, config.planActMaxSteps());
        assertTrue(config.llmDefaultHeaders().isEmpty());
        assertTrue(config.llmAdditionalKwargs().isEmpty());
        assertEquals(1, config.maxInFlightRecords());
    }

    @Test
    void rejectsBlankBaseUrlWithoutLeakingApiKey() {
        Properties properties = validProperties();
        properties.setProperty("llm.base-url", "");
        properties.setProperty("llm.api-key", "do-not-print-this");

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> SupportJobConfig.fromProperties(properties));

        assertTrue(exception.getMessage().contains("llm.base-url"));
        assertTrue(!exception.getMessage().contains("do-not-print-this"));
    }

    @Test
    void rejectsCacheTtlLongerThanMemoryTtl() {
        Properties properties = validProperties();
        properties.setProperty("cache.ttl-seconds", "601");

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> SupportJobConfig.fromProperties(properties));

        assertTrue(exception.getMessage().contains("cache.ttl-seconds"));
    }

    @Test
    void parsesDefaultHeaders() {
        Properties properties = validProperties();
        properties.setProperty(
                "llm.default-headers",
                "{\"x-opencode-session\":\"session-1\",\"User-Agent\":\"test-agent/1.0\"}");

        SupportJobConfig config = SupportJobConfig.fromProperties(properties);

        assertEquals(
                Map.of(
                        "x-opencode-session", "session-1",
                        "User-Agent", "test-agent/1.0"),
                config.llmDefaultHeaders());
    }

    @Test
    void parsesAdditionalRequestArguments() {
        Properties properties = validProperties();
        properties.setProperty(
                "llm.additional-kwargs",
                "{\"chat_template_kwargs\":{\"enable_thinking\":false}}");

        SupportJobConfig config = SupportJobConfig.fromProperties(properties);

        assertEquals(
                Map.of("chat_template_kwargs", Map.of("enable_thinking", false)),
                config.llmAdditionalKwargs());
    }

    @Test
    void rejectsInvalidAdditionalRequestArguments() {
        Properties properties = validProperties();
        properties.setProperty("llm.additional-kwargs", "not-json");

        assertThrows(
                IllegalArgumentException.class,
                () -> SupportJobConfig.fromProperties(properties));
    }

    @Test
    void rejectsInvalidDefaultHeaders() {
        Properties properties = validProperties();
        properties.setProperty("llm.default-headers", "not-json");

        assertThrows(
                IllegalArgumentException.class,
                () -> SupportJobConfig.fromProperties(properties));
    }

    @Test
    void rejectsInvalidPlanActMaxSteps() {
        Properties properties = validProperties();
        properties.setProperty("planact.max-steps", "0");

        assertThrows(
                IllegalArgumentException.class,
                () -> SupportJobConfig.fromProperties(properties));

        properties.setProperty("planact.max-steps", "21");
        assertThrows(
                IllegalArgumentException.class,
                () -> SupportJobConfig.fromProperties(properties));
    }

    @Test
    void rejectsNonPositiveDemoRate() {
        Properties properties = validProperties();
        properties.setProperty("demo.records-per-second", "0");

        assertThrows(
                IllegalArgumentException.class,
                () -> SupportJobConfig.fromProperties(properties));
    }

    private static Properties validProperties() {
        Properties properties = new Properties();
        properties.setProperty("llm.base-url", "https://model.example/v1");
        properties.setProperty("llm.api-key", "test-key");
        properties.setProperty("llm.model", "gpt-4o-mini");
        properties.setProperty("llm.temperature", "0.2");
        properties.setProperty("llm.max-tokens", "800");
        properties.setProperty("agent.async-threads", "2");
        properties.setProperty("agent.max-in-flight-records", "1");
        properties.setProperty("agent.memory-ttl-seconds", "600");
        properties.setProperty("cache.ttl-seconds", "600");
        properties.setProperty("job.checkpoint.interval-ms", "10000");
        properties.setProperty("job.checkpoint.min-pause-ms", "5000");
        properties.setProperty("job.checkpoint.timeout-ms", "60000");
        properties.setProperty("demo.records-per-second", "1.0");
        return properties;
    }
}
