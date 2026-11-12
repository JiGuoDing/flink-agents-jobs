package nju.jgd.operator.state;

import nju.jgd.model.SupportRequest;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserSessionStateFunctionTest {

    @Test
    void tracksSequenceAndReturningUsersPerKey() throws Exception {
        UserSessionStateFunction function = new UserSessionStateFunction(600L);
        KeyedProcessOperator<String, SupportRequest, SupportRequest> operator =
                new KeyedProcessOperator<>(function);
        try (KeyedOneInputStreamOperatorTestHarness<String, SupportRequest, SupportRequest> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, SupportRequest::getUserId, Types.STRING)) {
            harness.open();

            harness.processElement(request("r1", "u1", "s1"), 1_000L);
            harness.processElement(request("r2", "u1", "s2"), 2_000L);
            harness.processElement(request("r3", "u2", "s1"), 3_000L);

            List<SupportRequest> output = harness.extractOutputValues();
            assertEquals(3, output.size());

            assertEquals(1L, output.get(0).getRequestSequence());
            assertFalse(output.get(0).isReturningUser());

            assertEquals(2L, output.get(1).getRequestSequence());
            assertTrue(output.get(1).isReturningUser());

            assertEquals(1L, output.get(2).getRequestSequence());
            assertFalse(output.get(2).isReturningUser());
        }
    }

    private static SupportRequest request(String requestId, String userId, String sessionId) {
        return new SupportRequest(requestId, userId, sessionId, "message " + requestId, false);
    }
}
