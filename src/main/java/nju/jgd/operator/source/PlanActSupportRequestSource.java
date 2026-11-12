package nju.jgd.operator.source;

import nju.jgd.model.SupportRequest;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.List;

/** Bounded local source for the Plan-Act after-sales demonstration. */
public final class PlanActSupportRequestSource {

    public static final String SOURCE_NAME = "plan-act-support-request-source";

    private static final List<SupportRequest> REQUESTS =
            List.of(
                    request(
                            "u-1001",
                            "s-1",
                            "请查询订单 ORD-1001 的物流状态，并告诉我预计送达时间。"),
                    request(
                            "u-1002",
                            "s-1",
                            "订单 ORD-1002 已签收，我想退款。请先确认订单状态，再判断退款条件，并告诉我下一步。"),
                    request(
                            "u-1003",
                            "s-1",
                            "请查询订单 ORD-9999 的物流状态；如果查不到，请直接说明。"));

    private PlanActSupportRequestSource() {}

    public static DataGeneratorSource<SupportRequest> create(double recordsPerSecond) {
        if (recordsPerSecond <= 0.0d) {
            throw new IllegalArgumentException("recordsPerSecond must be greater than zero");
        }
        return new DataGeneratorSource<>(
                (GeneratorFunction<Long, SupportRequest>) PlanActSupportRequestSource::createRequest,
                REQUESTS.size(),
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(SupportRequest.class));
    }

    private static SupportRequest createRequest(Long index) {
        SupportRequest template = REQUESTS.get(Math.toIntExact(index));
        return new SupportRequest(
                "plan-act-req-%03d".formatted(index + 1),
                template.getUserId(),
                template.getSessionId(),
                template.getMessage(),
                false);
    }

    private static SupportRequest request(String userId, String sessionId, String message) {
        return new SupportRequest("", userId, sessionId, message, false);
    }
}