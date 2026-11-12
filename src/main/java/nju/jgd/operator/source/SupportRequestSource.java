package nju.jgd.operator.source;

import nju.jgd.model.SupportRequest;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.List;

/** Bounded local source for the customer-support demonstration. */
public final class SupportRequestSource {

    public static final String SOURCE_NAME = "customer-support-request-source";

    private static final List<SupportRequest> REQUESTS =
            List.of(
                    request("u-1001", "s-1", "查询订单 ORD-1001 的物流状态。", false),
                    request("u-1001", "s-1", "如果一直不到，可以退款吗？", false),
                    request("u-1001", "s-2", "你们的退货政策是什么？", true),
                    request("u-1001", "s-2", "退货运费谁承担？", false),
                    request("u-1001", "s-3", "你们的退货政策是什么？", true),
                    request("u-2002", "s-1", "查询订单 ORD-1002 的状态。", false),
                    request("u-2002", "s-1", "已经签收了，我想退款，应该怎么处理？", false),
                    request("u-2002", "s-2", "你们的退货政策是什么？", true),
                    request("u-1001", "s-2", "刚才那个订单的退款大概多久到账？", false),
                    request("u-3003", "s-1", "订单 ORD-9999 现在到哪了？", false));

    private SupportRequestSource() {}

    /**
     * Creates the bounded demonstration source.
     *
     * @param recordsPerSecond global source rate
     * @return DataGeneratorSource
     */
    public static DataGeneratorSource<SupportRequest> create(double recordsPerSecond) {
        if (recordsPerSecond <= 0.0d) {
            throw new IllegalArgumentException("recordsPerSecond must be greater than zero");
        }
        return new DataGeneratorSource<>(
                (GeneratorFunction<Long, SupportRequest>) SupportRequestSource::createRequest,
                REQUESTS.size(),
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(SupportRequest.class));
    }

    private static SupportRequest createRequest(Long index) {
        SupportRequest template = REQUESTS.get(Math.toIntExact(index));
        SupportRequest request = new SupportRequest(
                "req-%03d".formatted(index + 1),
                template.getUserId(),
                template.getSessionId(),
                template.getMessage(),
                template.isCacheable());
        return request;
    }

    private static SupportRequest request(
            String userId, String sessionId, String message, boolean cacheable) {
        return new SupportRequest("", userId, sessionId, message, cacheable);
    }
}


