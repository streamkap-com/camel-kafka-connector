package org.apache.camel.kafkaconnector.nettyhttp.routing.stripe;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StripePayloadStrategyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StripePayloadStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new StripePayloadStrategy();
        strategy.configure("stripe_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        strategy.configureAdvanced(Collections.emptySet(), false, "", true, null);
    }

    // --- Basic Routing ---

    @Test
    void testCustomerCreated() {
        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("stripe_customer", records.get(0).getTopic());
        assertEquals("customer.created", records.get(0).getEventType());
        assertEquals("cus_123", records.get(0).getKeyFields().get("id"));
        assertEquals("c", records.get(0).getOp());
        assertPayloadContains(records.get(0), "__changeType", "CREATE");
        assertPayloadContains(records.get(0), "__deleted", false);
    }

    @Test
    void testCustomerUpdated() {
        Map<String, Object> payload = stripeEvent("customer.updated", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertPayloadContains(records.get(0), "__changeType", "UPDATE");
        assertPayloadContains(records.get(0), "__deleted", false);
        assertEquals("u", records.get(0).getOp());
    }

    @Test
    void testCustomerDeleted() {
        Map<String, Object> payload = stripeEvent("customer.deleted", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertPayloadContains(records.get(0), "__changeType", "DELETE");
        assertPayloadContains(records.get(0), "__deleted", true);
        assertEquals("d", records.get(0).getOp());
    }

    // --- Payment Events ---

    @Test
    void testPaymentIntentSucceeded() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "pi_123");
        obj.put("amount", 2000);
        obj.put("currency", "usd");
        Map<String, Object> payload = stripeEvent("payment_intent.succeeded", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("stripe_payment_intent", records.get(0).getTopic());
        assertPayloadContains(records.get(0), "__changeType", "SUCCEEDED");
        assertEquals("u", records.get(0).getOp());
    }

    @Test
    void testPaymentIntentFailed() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "pi_456");
        Map<String, Object> payload = stripeEvent("payment_intent.payment_failed", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("stripe_payment_intent", records.get(0).getTopic());
        assertPayloadContains(records.get(0), "__changeType", "FAILED");
    }

    // --- Invoice Events ---

    @Test
    void testInvoicePaid() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "in_789");
        Map<String, Object> payload = stripeEvent("invoice.paid", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("stripe_invoice", records.get(0).getTopic());
        assertPayloadContains(records.get(0), "__changeType", "PAID");
    }

    // --- Multi-Level Event Types ---

    @Test
    void testCustomerSubscriptionUpdated() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "sub_abc");
        Map<String, Object> payload = stripeEvent("customer.subscription.updated", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("stripe_customer", records.get(0).getTopic());
        assertEquals("customer.subscription.updated", records.get(0).getEventType());
        assertPayloadContains(records.get(0), "__changeType", "UPDATE");
        assertEquals("sub_abc", records.get(0).getKeyFields().get("id"));
    }

    @Test
    void testChargeDisputeCreated() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "dp_xyz");
        Map<String, Object> payload = stripeEvent("charge.dispute.created", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("stripe_charge", records.get(0).getTopic());
        assertPayloadContains(records.get(0), "__changeType", "CREATE");
        assertEquals("c", records.get(0).getOp());
    }

    // --- Payload Extraction ---

    @Test
    void testDataObjectExtracted() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "cus_123");
        obj.put("email", "test@example.com");
        obj.put("name", "John Doe");
        Map<String, Object> payload = stripeEvent("customer.created", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertPayloadContains(records.get(0), "id", "cus_123");
        assertPayloadContains(records.get(0), "email", "test@example.com");
        assertPayloadContains(records.get(0), "name", "John Doe");
        // Envelope fields should NOT be in output
        assertPayloadNotContains(records.get(0), "object");
        assertPayloadNotContains(records.get(0), "pending_webhooks");
    }

    // --- Event Metadata ---

    @Test
    void testIncludeEventTrue() {
        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertPayloadContains(records.get(0), "_event_id", "evt_test");
        assertPayloadContains(records.get(0), "_event_type", "customer.created");
        assertPayloadContains(records.get(0), "_livemode", false);
    }

    @Test
    void testIncludeEventFalse() {
        strategy.configureAdvanced(Collections.emptySet(), false, "", false, null);

        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertPayloadNotContains(records.get(0), "_event_id");
        assertPayloadNotContains(records.get(0), "_event_type");
        assertPayloadNotContains(records.get(0), "_livemode");
    }

    // --- Allowed Objects Filter ---

    @Test
    void testAllowedObjectsAccepts() {
        strategy.configureAdvanced(Collections.emptySet(), false, "", true,
                new HashSet<>(Arrays.asList("customer", "payment_intent")));

        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("stripe_customer", records.get(0).getTopic());
    }

    @Test
    void testAllowedObjectsRejects() {
        strategy.configureAdvanced(Collections.emptySet(), false, "", true,
                new HashSet<>(Collections.singletonList("customer")));

        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "pi_123");
        Map<String, Object> payload = stripeEvent("payment_intent.succeeded", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("stripe_unknown", records.get(0).getTopic());
    }

    // --- Unknown/Error Handling ---

    @Test
    void testMissingTypeField() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", "evt_test");
        payload.put("data", Map.of("object", Map.of("id", "cus_123")));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("stripe_unknown", records.get(0).getTopic());
    }

    @Test
    void testMissingDataObject() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "customer.created");
        payload.put("data", Map.of());

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("stripe_unknown", records.get(0).getTopic());
    }

    @Test
    void testSkipBehavior() {
        strategy.configure("stripe_", UnknownTypeBehavior.SKIP, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", "evt_test");

        List<RoutedRecord> records = strategy.route(payload);

        assertTrue(records.isEmpty());
    }

    @Test
    void testFailBehavior() {
        strategy.configure("stripe_", UnknownTypeBehavior.FAIL, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", "evt_test");

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // --- Signature Verification ---

    @Test
    void testSignatureVerificationSuccess() throws Exception {
        String secret = "whsec_test_secret";
        strategy.configureHmac(secret);
        strategy.setTimestampToleranceSeconds(0); // Disable timestamp check for test

        String body = MAPPER.writeValueAsString(stripeEvent("customer.created", customerObject("cus_123")));
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
        String signature = computeStripeSignature(timestamp, body, secret);

        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));
        Map<String, Object> headers = new HashMap<>();
        headers.put(StripePayloadStrategy.HEADER_SIGNATURE, "t=" + timestamp + ",v1=" + signature);
        headers.put("__rawBody", body);

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("stripe_customer", records.get(0).getTopic());
    }

    @Test
    void testSignatureVerificationFailure() {
        strategy.configureHmac("whsec_test_secret");
        strategy.setTimestampToleranceSeconds(0);

        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));
        Map<String, Object> headers = new HashMap<>();
        headers.put(StripePayloadStrategy.HEADER_SIGNATURE, "t=1234567890,v1=invalidsignature");
        headers.put("__rawBody", "{}");

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload, headers));
    }

    @Test
    void testSignatureMissingHeader() {
        strategy.configureHmac("whsec_test_secret");

        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));
        Map<String, Object> headers = new HashMap<>();
        headers.put("__rawBody", "{}");

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload, headers));
    }

    @Test
    void testSignatureNotConfiguredSkips() {
        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
    }

    // --- Fan-Out ---

    @Test
    void testInvoiceLinesFanout() {
        strategy.configureAdvanced(new HashSet<>(Collections.singletonList("invoice.lines")),
                false, "", true, null);

        Map<String, Object> lineItem1 = new LinkedHashMap<>();
        lineItem1.put("id", "il_1");
        lineItem1.put("amount", 1000);
        Map<String, Object> lineItem2 = new LinkedHashMap<>();
        lineItem2.put("id", "il_2");
        lineItem2.put("amount", 500);

        Map<String, Object> lines = new LinkedHashMap<>();
        lines.put("object", "list");
        lines.put("data", Arrays.asList(lineItem1, lineItem2));

        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", "in_789");
        obj.put("lines", lines);

        Map<String, Object> payload = stripeEvent("invoice.paid", obj);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(3, records.size());
        assertEquals("stripe_invoice", records.get(0).getTopic());
        assertEquals("stripe_invoice_lines", records.get(1).getTopic());
        assertEquals("stripe_invoice_lines", records.get(2).getTopic());
        assertEquals("in_789", records.get(1).getKeyFields().get("id"));
        assertEquals("il_1", records.get(1).getKeyFields().get("item_id"));
    }

    // --- Change Type Static Methods ---

    @Test
    void testMapActionToChangeType() {
        assertEquals("CREATE", StripePayloadStrategy.mapActionToChangeType("created"));
        assertEquals("UPDATE", StripePayloadStrategy.mapActionToChangeType("updated"));
        assertEquals("DELETE", StripePayloadStrategy.mapActionToChangeType("deleted"));
        assertEquals("SUCCEEDED", StripePayloadStrategy.mapActionToChangeType("succeeded"));
        assertEquals("FAILED", StripePayloadStrategy.mapActionToChangeType("failed"));
        assertEquals("CANCELED", StripePayloadStrategy.mapActionToChangeType("canceled"));
        assertEquals("PAID", StripePayloadStrategy.mapActionToChangeType("paid"));
        assertEquals("FINALIZED", StripePayloadStrategy.mapActionToChangeType("finalized"));
        assertEquals("UNKNOWN", StripePayloadStrategy.mapActionToChangeType(null));
    }

    @Test
    void testChangeTypeToOp() {
        assertEquals("c", StripePayloadStrategy.changeTypeToOp("CREATE"));
        assertEquals("d", StripePayloadStrategy.changeTypeToOp("DELETE"));
        assertEquals("u", StripePayloadStrategy.changeTypeToOp("UPDATE"));
        assertEquals("u", StripePayloadStrategy.changeTypeToOp("SUCCEEDED"));
        assertEquals("u", StripePayloadStrategy.changeTypeToOp(null));
    }

    // --- Topic Prefix ---

    @Test
    void testCustomPrefix() {
        strategy.configure("myapp_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("myapp_customer", records.get(0).getTopic());
    }

    @Test
    void testEmptyPrefix() {
        strategy.configure("", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = stripeEvent("customer.created", customerObject("cus_123"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("customer", records.get(0).getTopic());
    }

    // --- Helpers ---

    private static Map<String, Object> stripeEvent(String type, Map<String, Object> dataObject) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("id", "evt_test");
        event.put("object", "event");
        event.put("api_version", "2023-10-16");
        event.put("created", 1680064028);
        event.put("livemode", false);
        event.put("pending_webhooks", 1);
        event.put("type", type);
        event.put("data", Map.of("object", dataObject));
        return event;
    }

    private static Map<String, Object> customerObject(String id) {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", id);
        obj.put("email", "test@example.com");
        obj.put("name", "Test Customer");
        return obj;
    }

    @SuppressWarnings("unchecked")
    private static void assertPayloadContains(RoutedRecord record, String key, Object expected) {
        try {
            Map<String, Object> payload = new ObjectMapper().readValue(record.getPayload(), Map.class);
            assertTrue(payload.containsKey(key), "Payload should contain key: " + key);
            assertEquals(expected, payload.get(key), "Payload field '" + key + "' mismatch");
        } catch (Exception e) {
            fail("Failed to parse payload: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertPayloadNotContains(RoutedRecord record, String key) {
        try {
            Map<String, Object> payload = new ObjectMapper().readValue(record.getPayload(), Map.class);
            assertFalse(payload.containsKey(key), "Payload should not contain key: " + key);
        } catch (Exception e) {
            fail("Failed to parse payload: " + e.getMessage());
        }
    }

    private static String computeStripeSignature(String timestamp, String body, String secret) throws Exception {
        String signedPayload = timestamp + "." + body;
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        mac.init(key);
        byte[] hash = mac.doFinal(signedPayload.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
