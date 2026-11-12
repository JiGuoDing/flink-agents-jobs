package nju.jgd.operator.state;

import nju.jgd.model.SupportRequest;
import nju.jgd.model.UserSessionState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Maintains explicit Flink keyed state for each user.
 *
 * <p>This state survives checkpoints and records cross-session user activity before the record is
 * passed to the Flink Agents operator.
 */
public final class UserSessionStateFunction
        extends KeyedProcessFunction<String, SupportRequest, SupportRequest> {

    private static final String STATE_NAME = "user-session-state";

    private final long stateTtlSeconds;
    private transient ValueState<UserSessionState> userState;

    public UserSessionStateFunction(long stateTtlSeconds) {
        if (stateTtlSeconds <= 0L) {
            throw new IllegalArgumentException("stateTtlSeconds must be greater than zero");
        }
        this.stateTtlSeconds = stateTtlSeconds;
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<UserSessionState> descriptor =
                new ValueStateDescriptor<>(STATE_NAME, UserSessionState.class);
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Duration.ofSeconds(stateTtlSeconds))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
        descriptor.enableTimeToLive(ttlConfig);
        userState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            SupportRequest request, Context context, Collector<SupportRequest> collector)
            throws Exception {
        Long timestamp = context.timestamp();
        long now = timestamp != null ? timestamp : System.currentTimeMillis();
        UserSessionState previous = userState.value();

        UserSessionState updated = new UserSessionState();
        updated.setFirstSeenAt(previous == null ? now : previous.getFirstSeenAt());
        updated.setLastSeenAt(now);
        updated.setRequestCount(previous == null ? 1L : previous.getRequestCount() + 1L);
        updated.setLastSessionId(request.getSessionId());
        userState.update(updated);

        request.setReturningUser(previous != null);
        request.setRequestSequence(updated.getRequestCount());
        collector.collect(request);
    }
}

