package org.apache.camel.kafkaconnector.nettyhttp.routing.shopify;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ShopifyPayloadStrategyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ShopifyPayloadStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new ShopifyPayloadStrategy();
        strategy.configure("shopify_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        strategy.configureAdvanced(Collections.emptySet(), false, "", true, null);
    }

    // --- Basic Routing ---

    @Test
    void testOrderCreated() {
        Map<String, Object> payload = orderPayload(12345L);
        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_orders", records.get(0).getTopic());
        assertEquals("orders.create", records.get(0).getEventType());
        assertEquals(12345L, records.get(0).getKeyFields().get("id"));
        assertPayloadContains(records.get(0), "__changeType", "CREATE");
        assertPayloadContains(records.get(0), "__deleted", false);
    }

    @Test
    void testOrderUpdated() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/updated");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertPayloadContains(records.get(0), "__changeType", "UPDATE");
        assertPayloadContains(records.get(0), "__deleted", false);
    }

    @Test
    void testOrderDeleted() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/delete");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertPayloadContains(records.get(0), "__changeType", "DELETE");
        assertPayloadContains(records.get(0), "__deleted", true);
    }

    @Test
    void testProductCreated() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 9999L);
        payload.put("title", "Widget");
        Map<String, Object> headers = shopifyHeaders("products/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_products", records.get(0).getTopic());
        assertEquals(9999L, records.get(0).getKeyFields().get("id"));
    }

    @Test
    void testCustomerDeleted() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 555L);
        Map<String, Object> headers = shopifyHeaders("customers/delete");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_customers", records.get(0).getTopic());
        assertPayloadContains(records.get(0), "__deleted", true);
        assertPayloadContains(records.get(0), "__changeType", "DELETE");
    }

    // --- Shopify-Specific Actions ---

    @Test
    void testOrderCancelled() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/cancelled");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertPayloadContains(records.get(0), "__changeType", "CANCELLED");
        assertPayloadContains(records.get(0), "__deleted", false);
    }

    @Test
    void testOrderFulfilled() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/fulfilled");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertPayloadContains(records.get(0), "__changeType", "FULFILLED");
        assertPayloadContains(records.get(0), "__deleted", false);
    }

    @Test
    void testOrderPaid() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/paid");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertPayloadContains(records.get(0), "__changeType", "PAID");
    }

    @Test
    void testOrderPartiallyFulfilled() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/partially_fulfilled");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertPayloadContains(records.get(0), "__changeType", "PARTIALLY_FULFILLED");
    }

    @Test
    void testUnknownActionUppercased() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/edited");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertPayloadContains(records.get(0), "__changeType", "EDITED");
        assertPayloadContains(records.get(0), "__deleted", false);
    }

    // --- Dot-Separated Topics ---

    @Test
    void testDotSeparatedTopic() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 777L);
        Map<String, Object> headers = shopifyHeaders("customer.tags_added");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_customer", records.get(0).getTopic());
        assertEquals("customer.tags_added", records.get(0).getEventType());
        assertPayloadContains(records.get(0), "__changeType", "TAGS_ADDED");
    }

    // --- Fan-Out ---

    @Test
    void testOrderWithLineItemsFanout() {
        Set<String> fanout = new HashSet<>();
        fanout.add("orders.line_items");
        strategy.configureAdvanced(fanout, false, "", true, null);

        Map<String, Object> payload = orderWithLineItems(100L, 2);
        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(3, records.size()); // 1 main + 2 line items
        assertEquals("shopify_orders", records.get(0).getTopic());
        assertEquals("shopify_orders_line_items", records.get(1).getTopic());
        assertEquals("shopify_orders_line_items", records.get(2).getTopic());
    }

    @Test
    void testFanoutRecordHasCompositeKey() {
        Set<String> fanout = new HashSet<>();
        fanout.add("orders.line_items");
        strategy.configureAdvanced(fanout, false, "", true, null);

        Map<String, Object> payload = orderWithLineItems(100L, 1);
        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(2, records.size());
        // Fan-out record has composite key
        RoutedRecord fanoutRecord = records.get(1);
        assertEquals(100L, fanoutRecord.getKeyFields().get("id"));
        assertEquals(1L, fanoutRecord.getKeyFields().get("item_id"));
    }

    @Test
    void testFanoutRecordHasParentIdAndContext() {
        Set<String> fanout = new HashSet<>();
        fanout.add("orders.line_items");
        strategy.configureAdvanced(fanout, false, "", true, null);

        Map<String, Object> payload = orderWithLineItems(100L, 1);
        Map<String, Object> headers = shopifyHeaders("orders/create");
        headers.put(ShopifyPayloadStrategy.HEADER_EVENT_ID, "evt-456");
        headers.put(ShopifyPayloadStrategy.HEADER_SHOP_DOMAIN, "test.myshopify.com");

        List<RoutedRecord> records = strategy.route(payload, headers);

        RoutedRecord fanoutRecord = records.get(1);
        assertPayloadContainsNumber(fanoutRecord, "id", 100);
        assertPayloadContains(fanoutRecord, "_ctx_event_id", "evt-456");
        assertPayloadContains(fanoutRecord, "_ctx_shop_domain", "test.myshopify.com");
    }

    @Test
    void testProductVariantsFanout() {
        Set<String> fanout = new HashSet<>();
        fanout.add("products.variants");
        strategy.configureAdvanced(fanout, false, "", true, null);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 200L);
        payload.put("title", "T-Shirt");
        List<Map<String, Object>> variants = new ArrayList<>();
        Map<String, Object> v1 = new LinkedHashMap<>();
        v1.put("id", 10L);
        v1.put("title", "Small");
        variants.add(v1);
        payload.put("variants", variants);

        Map<String, Object> headers = shopifyHeaders("products/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(2, records.size());
        assertEquals("shopify_products", records.get(0).getTopic());
        assertEquals("shopify_products_variants", records.get(1).getTopic());
        assertEquals(200L, records.get(1).getKeyFields().get("id"));
        assertEquals(10L, records.get(1).getKeyFields().get("item_id"));
    }

    @Test
    void testEmptyArrayNoFanoutRecords() {
        Set<String> fanout = new HashSet<>();
        fanout.add("orders.line_items");
        strategy.configureAdvanced(fanout, false, "", true, null);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 100L);
        payload.put("line_items", Collections.emptyList());

        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size()); // Only main record
    }

    @Test
    void testFanoutIsDomainScoped() {
        Set<String> fanout = new HashSet<>();
        fanout.add("orders.line_items");
        strategy.configureAdvanced(fanout, false, "", true, null);

        // Products also have line-item-like arrays, but "orders.line_items" shouldn't trigger on products
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 200L);
        List<Map<String, Object>> items = new ArrayList<>();
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", 1L);
        items.add(item);
        payload.put("line_items", items);

        Map<String, Object> headers = shopifyHeaders("products/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size()); // No fanout for products.line_items
    }

    @Test
    void testMultipleFanoutFields() {
        Set<String> fanout = new HashSet<>();
        fanout.add("orders.line_items");
        fanout.add("orders.shipping_lines");
        strategy.configureAdvanced(fanout, false, "", true, null);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 100L);
        List<Map<String, Object>> lineItems = new ArrayList<>();
        lineItems.add(mapOf("id", 1L, "title", "Item 1"));
        payload.put("line_items", lineItems);
        List<Map<String, Object>> shippingLines = new ArrayList<>();
        shippingLines.add(mapOf("id", 10L, "title", "Standard"));
        payload.put("shipping_lines", shippingLines);

        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(3, records.size()); // 1 main + 1 line_item + 1 shipping_line
        assertEquals("shopify_orders", records.get(0).getTopic());
        assertEquals("shopify_orders_line_items", records.get(1).getTopic());
        assertEquals("shopify_orders_shipping_lines", records.get(2).getTopic());
    }

    // --- Include Event Metadata ---

    @Test
    void testIncludeEventTrue() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/create");
        headers.put(ShopifyPayloadStrategy.HEADER_SHOP_DOMAIN, "test.myshopify.com");
        headers.put(ShopifyPayloadStrategy.HEADER_EVENT_ID, "evt-123");
        headers.put(ShopifyPayloadStrategy.HEADER_TRIGGERED_AT, "2026-05-11T10:00:00Z");
        headers.put(ShopifyPayloadStrategy.HEADER_API_VERSION, "2024-01");
        headers.put(ShopifyPayloadStrategy.HEADER_WEBHOOK_ID, "wh-456");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertPayloadContains(records.get(0), "_shop_domain", "test.myshopify.com");
        assertPayloadContains(records.get(0), "_event_id", "evt-123");
        assertPayloadContains(records.get(0), "_triggered_at", "2026-05-11T10:00:00Z");
        assertPayloadContains(records.get(0), "_api_version", "2024-01");
        assertPayloadContains(records.get(0), "_webhook_id", "wh-456");
    }

    @Test
    void testIncludeEventFalse() {
        strategy.configureAdvanced(Collections.emptySet(), false, "", false, null);

        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/create");
        headers.put(ShopifyPayloadStrategy.HEADER_SHOP_DOMAIN, "test.myshopify.com");
        headers.put(ShopifyPayloadStrategy.HEADER_EVENT_ID, "evt-123");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertPayloadNotContains(records.get(0), "_shop_domain");
        assertPayloadNotContains(records.get(0), "_event_id");
        assertPayloadNotContains(records.get(0), "_triggered_at");
    }

    // --- Allowed Objects Filter ---

    @Test
    void testAllowedObjectsAccepts() {
        Set<String> allowed = new HashSet<>(Arrays.asList("orders", "products"));
        strategy.configureAdvanced(Collections.emptySet(), false, "", true, allowed);

        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_orders", records.get(0).getTopic());
    }

    @Test
    void testAllowedObjectsRejects() {
        Set<String> allowed = new HashSet<>(Collections.singletonList("orders"));
        strategy.configureAdvanced(Collections.emptySet(), false, "", true, allowed);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 200L);
        Map<String, Object> headers = shopifyHeaders("products/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_unknown", records.get(0).getTopic()); // Routed to default
    }

    // --- Unknown/Error Handling ---

    @Test
    void testMissingTopicHeader() {
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = new HashMap<>(); // No X-Shopify-Topic

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_unknown", records.get(0).getTopic());
    }

    @Test
    void testMissingTopicHeaderSkipBehavior() {
        strategy.configure("shopify_", UnknownTypeBehavior.SKIP, "unknown");

        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = new HashMap<>();

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertTrue(records.isEmpty());
    }

    @Test
    void testMissingTopicHeaderFailBehavior() {
        strategy.configure("shopify_", UnknownTypeBehavior.FAIL, "unknown");

        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = new HashMap<>();

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload, headers));
    }

    @Test
    void testBodyOnlyRouteFallback() {
        Map<String, Object> payload = orderPayload(100L);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("shopify_unknown", records.get(0).getTopic());
    }

    // --- HMAC Verification ---

    @Test
    void testHmacVerificationSuccess() throws Exception {
        String secret = "test-secret-key";
        strategy.configureHmac(secret);

        String body = "{\"id\":100}";
        String hmac = computeHmac(body, secret);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 100);
        Map<String, Object> headers = shopifyHeaders("orders/create");
        headers.put(ShopifyPayloadStrategy.HEADER_HMAC, hmac);
        headers.put("__rawBody", body);

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertEquals("shopify_orders", records.get(0).getTopic());
    }

    @Test
    void testHmacVerificationFailure() throws Exception {
        String secret = "test-secret-key";
        strategy.configureHmac(secret);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 100);
        Map<String, Object> headers = shopifyHeaders("orders/create");
        headers.put(ShopifyPayloadStrategy.HEADER_HMAC, "invalid-hmac-value");
        headers.put("__rawBody", "{\"id\":100}");

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload, headers));
    }

    @Test
    void testHmacMissingHeader() {
        strategy.configureHmac("test-secret");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 100);
        Map<String, Object> headers = shopifyHeaders("orders/create");
        headers.put("__rawBody", "{\"id\":100}");
        // No HMAC header

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload, headers));
    }

    @Test
    void testHmacNotConfiguredSkipsVerification() {
        // No configureHmac() call — HMAC should be skipped
        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/create");
        // No HMAC header at all

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size()); // Should work fine without HMAC
    }

    // --- Key Schema ---

    @Test
    void testMissingIdEmptyKey() {
        Map<String, Object> payload = new LinkedHashMap<>(); // No id field
        payload.put("name", "Shop Update");
        Map<String, Object> headers = shopifyHeaders("shop/update");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals(1, records.size());
        assertTrue(records.get(0).getKeyFields().isEmpty());
    }

    // --- Topic Prefix ---

    @Test
    void testCustomPrefix() {
        strategy.configure("myapp_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals("myapp_orders", records.get(0).getTopic());
    }

    @Test
    void testEmptyPrefix() {
        strategy.configure("", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = orderPayload(100L);
        Map<String, Object> headers = shopifyHeaders("orders/create");

        List<RoutedRecord> records = strategy.route(payload, headers);

        assertEquals("orders", records.get(0).getTopic());
    }

    // --- Change Type Static Methods ---

    @Test
    void testMapActionToChangeType() {
        assertEquals("CREATE", ShopifyPayloadStrategy.mapActionToChangeType("create"));
        assertEquals("UPDATE", ShopifyPayloadStrategy.mapActionToChangeType("update"));
        assertEquals("UPDATE", ShopifyPayloadStrategy.mapActionToChangeType("updated"));
        assertEquals("DELETE", ShopifyPayloadStrategy.mapActionToChangeType("delete"));
        assertEquals("CANCELLED", ShopifyPayloadStrategy.mapActionToChangeType("cancelled"));
        assertEquals("FULFILLED", ShopifyPayloadStrategy.mapActionToChangeType("fulfilled"));
        assertEquals("PAID", ShopifyPayloadStrategy.mapActionToChangeType("paid"));
        assertEquals("PARTIALLY_FULFILLED", ShopifyPayloadStrategy.mapActionToChangeType("partially_fulfilled"));
        assertEquals("EDITED", ShopifyPayloadStrategy.mapActionToChangeType("edited"));
        assertEquals("UNKNOWN", ShopifyPayloadStrategy.mapActionToChangeType(null));
    }

    @Test
    void testIsDeleteAction() {
        assertTrue(ShopifyPayloadStrategy.isDeleteAction("delete"));
        assertTrue(ShopifyPayloadStrategy.isDeleteAction("DELETE"));
        assertFalse(ShopifyPayloadStrategy.isDeleteAction("create"));
        assertFalse(ShopifyPayloadStrategy.isDeleteAction("cancelled"));
        assertFalse(ShopifyPayloadStrategy.isDeleteAction(null));
    }

    // --- Helpers ---

    private static Map<String, Object> shopifyHeaders(String topic) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(ShopifyPayloadStrategy.HEADER_TOPIC, topic);
        return headers;
    }

    private static Map<String, Object> orderPayload(long id) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", id);
        payload.put("email", "test@example.com");
        payload.put("total_price", "29.99");
        return payload;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> orderWithLineItems(long orderId, int itemCount) {
        Map<String, Object> payload = orderPayload(orderId);
        List<Map<String, Object>> lineItems = new ArrayList<>();
        for (int i = 1; i <= itemCount; i++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", (long) i);
            item.put("title", "Item " + i);
            item.put("quantity", 1);
            lineItems.add(item);
        }
        payload.put("line_items", lineItems);
        return payload;
    }

    private static Map<String, Object> mapOf(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    @SuppressWarnings("unchecked")
    private static void assertPayloadContains(RoutedRecord record, String key, Object expectedValue) {
        try {
            Map<String, Object> payload = new ObjectMapper().readValue(record.getPayload(), Map.class);
            assertTrue(payload.containsKey(key), "Payload should contain key: " + key);
            assertEquals(expectedValue, payload.get(key),
                    "Payload field '" + key + "' should be " + expectedValue);
        } catch (Exception e) {
            fail("Failed to parse payload: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertPayloadContainsNumber(RoutedRecord record, String key, long expectedValue) {
        try {
            Map<String, Object> payload = new ObjectMapper().readValue(record.getPayload(), Map.class);
            assertTrue(payload.containsKey(key), "Payload should contain key: " + key);
            assertEquals(expectedValue, ((Number) payload.get(key)).longValue(),
                    "Payload field '" + key + "' should be " + expectedValue);
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

    private static String computeHmac(String body, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec secretKey = new SecretKeySpec(
                secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        mac.init(secretKey);
        byte[] hash = mac.doFinal(body.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }
}
