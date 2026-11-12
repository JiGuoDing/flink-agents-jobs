package nju.jgd.tool.support;

import org.apache.flink.agents.api.annotation.Tool;
import org.apache.flink.agents.api.annotation.ToolParam;

import java.util.Map;

/**
 * Deterministic local tools used by the example. Replace this repository with real service calls
 * when integrating a production order system.
 */
public final class OrderTools {

    private static final Map<String, OrderRecord> ORDERS =
            Map.of(
                    "ORD-1001",
                    new OrderRecord(
                            "ORD-1001",
                            "SHIPPED",
                            "SF Express",
                            "2026-11-08",
                            null,
                            "2026-11-15",
                            "199.00"),
                    "ORD-1002",
                    new OrderRecord(
                            "ORD-1002",
                            "DELIVERED",
                            "JD Logistics",
                            null,
                            "2026-11-01",
                            "2026-11-08",
                            "299.00"));

    private OrderTools() {}

    /** Looks up the current state of an order. */
    @Tool(description = "Look up order status, carrier, delivery estimate, and return deadline.")
    public static String lookupOrderStatus(
            @ToolParam(name = "orderId", description = "Order id, for example ORD-1001")
                    String orderId) {
        OrderRecord order = ORDERS.get(normalize(orderId));
        if (order == null) {
            return "orderId=" + orderId + "; found=false; message=Order not found";
        }
        return ("orderId=%s; status=%s; carrier=%s; estimatedDelivery=%s; deliveredAt=%s; "
                        + "returnDeadline=%s; orderAmount=%s")
                .formatted(
                        order.orderId,
                        order.status,
                        order.carrier,
                        nullSafe(order.estimatedDelivery),
                        nullSafe(order.deliveredAt),
                        order.returnDeadline,
                        order.orderAmount);
    }

    /** Returns deterministic refund eligibility and the next customer-service step. */
    @Tool(description = "Check refund eligibility for an order and explain the next step.")
    public static String checkRefundEligibility(
            @ToolParam(name = "orderId", description = "Order id, for example ORD-1001")
                    String orderId,
            @ToolParam(name = "reason", description = "Customer-provided refund reason")
                    String reason) {
        OrderRecord order = ORDERS.get(normalize(orderId));
        if (order == null) {
            return "orderId=" + orderId + "; found=false; eligible=false; message=Order not found";
        }

        String normalizedReason = reason == null ? "" : reason.trim();
        String category = normalizedReason.isEmpty() ? "UNSPECIFIED" : "CUSTOMER_REQUEST";
        String hold = "Inspect the order status with lookupOrderStatus before promising a refund.";
        if (!"ORD-1002".equals(order.orderId)) {
            hold = "The parcel is still in transit. Offer delivery follow-up before refunding.";
        }
        return ("orderId=%s; found=true; eligible=true; reasonCategory=%s; refundAmount=%s; "
                        + "nextStep=%s")
                .formatted(order.orderId, category, order.orderAmount, hold);
    }

    private static String normalize(String orderId) {
        return orderId == null ? "" : orderId.trim().toUpperCase();
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private record OrderRecord(
            String orderId,
            String status,
            String carrier,
            String estimatedDelivery,
            String deliveredAt,
            String returnDeadline,
            String orderAmount) {}
}

