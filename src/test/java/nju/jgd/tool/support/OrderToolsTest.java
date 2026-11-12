package nju.jgd.tool.support;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderToolsTest {

    @Test
    void lookupReturnsKnownOrderFacts() {
        String result = OrderTools.lookupOrderStatus("ORD-1001");

        assertTrue(result.contains("status=SHIPPED"));
        assertTrue(result.contains("SF Express"));
    }

    @Test
    void lookupMarksUnknownOrders() {
        String result = OrderTools.lookupOrderStatus("ORD-9999");

        assertTrue(result.contains("found=false"));
        assertTrue(result.contains("Order not found"));
    }

    @Test
    void refundCheckReturnsAmountAndNextStep() {
        String result = OrderTools.checkRefundEligibility("ORD-1002", "商品不合适");

        assertTrue(result.contains("eligible=true"));
        assertTrue(result.contains("refundAmount=299.00"));
        assertTrue(result.contains("nextStep="));
    }
}
