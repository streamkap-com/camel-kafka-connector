package org.apache.camel.kafkaconnector.nettyhttp.routing.salesforce;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRouter;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingStrategy;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SalesforcePayloadStrategyTest {

    private SalesforcePayloadStrategy strategy;
    private SalesforcePayloadStrategy flattenStrategy;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        strategy = new SalesforcePayloadStrategy();
        strategy.configure("sf_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        flattenStrategy = new SalesforcePayloadStrategy();
        flattenStrategy.configure("sf_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        flattenStrategy.configureAdvanced(Collections.emptySet(), true, "", false);
    }

    // ===================== CDC: CREATE =====================

    @Test
    void testCdcAccountCreated() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "CREATE",
                "001D000000KnaXjIAJ", Map.of(
                        "Name", "Acme",
                        "Phone", "(555) 555-5555",
                        "BillingCity", "San Francisco"
                ));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("sf_account_events", records.get(0).getTopic());
        assertEquals("Account.CREATE", records.get(0).getEventType());
        assertEquals("001D000000KnaXjIAJ", records.get(0).getKeyFields().get("Id"));

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals("001D000000KnaXjIAJ", result.get("Id"));
        assertEquals("Acme", result.get("Name"));
        assertEquals(false, result.get("__deleted"));
        assertNotNull(result.get("ChangeEventHeader"));
    }

    @Test
    void testCdcContactCreated() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Contact", "CREATE",
                "003xx000001", Map.of("FirstName", "Jane", "LastName", "Smith"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("sf_contact_events", records.get(0).getTopic());
        assertEquals("003xx000001", records.get(0).getKeyFields().get("Id"));
    }

    @Test
    void testCdcCustomObjectCreated() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Invoice__c", "CREATE",
                "a00xx000001", Map.of("Amount__c", 500.0));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("sf_invoice__c_events", records.get(0).getTopic());
    }

    // ===================== CDC: UPDATE =====================

    @Test
    void testCdcAccountUpdated() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "UPDATE",
                "001D000000KnaXjIAJ", Map.of("Name", "Acme Corp"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("sf_account_events", records.get(0).getTopic());
        assertEquals("Account.UPDATE", records.get(0).getEventType());

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(false, result.get("__deleted"));
    }

    // ===================== CDC: DELETE =====================

    @Test
    void testCdcAccountDeleted() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "DELETE",
                "001D000000KnaXjIAJ", Collections.emptyMap());

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("sf_account_events", records.get(0).getTopic());

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(true, result.get("__deleted"));
    }

    // ===================== CDC: UNDELETE =====================

    @Test
    void testCdcAccountUndeleted() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "UNDELETE",
                "001D000000KnaXjIAJ", Map.of("Name", "Acme"));

        List<RoutedRecord> records = strategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(false, result.get("__deleted"));
    }

    // ===================== CDC: GAP EVENTS =====================

    @Test
    void testCdcGapCreate() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "GAP_CREATE",
                "001D000000KnaXjIAJ", Collections.emptyMap());

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("sf_gap_events", records.get(0).getTopic());
        assertEquals("Account.GAP_CREATE", records.get(0).getEventType());

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(true, result.get("_gap"));
        assertEquals("Account", result.get("entityName"));
        assertEquals(false, result.get("__deleted"));
    }

    @Test
    void testCdcGapDelete() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "GAP_DELETE",
                "001D000000KnaXjIAJ", Collections.emptyMap());

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("sf_gap_events", records.get(0).getTopic());

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(true, result.get("__deleted"));
    }

    @Test
    void testCdcGapOverflow() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "GAP_OVERFLOW",
                null, Collections.emptyMap());

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("sf_gap_events", records.get(0).getTopic());
        assertFalse(records.get(0).hasKey());
    }

    // ===================== CDC: Record ID extraction =====================

    @Test
    void testCdcRecordIdFromPayload() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Lead", "CREATE",
                "00Qxx000001", Map.of("Company", "Test"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("00Qxx000001", records.get(0).getKeyFields().get("Id"));
    }

    @Test
    void testCdcRecordIdFallbackToRecordIds() throws Exception {
        // Build CDC payload without Id in payload body, only in recordIds
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("entityName", "Account");
        header.put("changeType", "DELETE");
        header.put("recordIds", Arrays.asList("001xx000002"));

        Map<String, Object> cdcPayload = new LinkedHashMap<>();
        cdcPayload.put("ChangeEventHeader", header);
        // No Id field in payload (typical for DELETE)

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("replayId", 10);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("payload", cdcPayload);
        data.put("event", event);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", data);
        payload.put("channel", "/data/AccountChangeEvent");

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("001xx000002", records.get(0).getKeyFields().get("Id"));
    }

    // ===================== CDC: Flattening =====================

    @Test
    void testCdcFlatten() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "CREATE",
                "001D000000KnaXjIAJ", Map.of("Name", "Acme", "Phone", "(555) 555-5555"));

        List<RoutedRecord> records = flattenStrategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);

        // Fields promoted to top level (no prefix since flattenDetailPrefix is "")
        assertEquals("001D000000KnaXjIAJ", result.get("Id"));
        assertEquals("Acme", result.get("Name"));
        assertEquals(false, result.get("__deleted"));

        // ChangeEventHeader excluded (includeEvent=false)
        assertNull(result.get("ChangeEventHeader"));
        // But changeType metadata is NOT included when includeEvent=false
        assertNull(result.get("changeType"));

        // replayId still included
        assertNotNull(result.get("replayId"));
    }

    @Test
    void testCdcFlattenWithPrefix() throws Exception {
        SalesforcePayloadStrategy prefixStrategy = new SalesforcePayloadStrategy();
        prefixStrategy.configure("sf_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        prefixStrategy.configureAdvanced(Collections.emptySet(), true, "sf_", true);

        Map<String, Object> payload = buildCdcPayload("Account", "CREATE",
                "001D000000KnaXjIAJ", Map.of("Name", "Acme"));

        List<RoutedRecord> records = prefixStrategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);

        assertEquals("001D000000KnaXjIAJ", result.get("sf_Id"));
        assertEquals("Acme", result.get("sf_Name"));
        // Key matches flattened field
        assertEquals("001D000000KnaXjIAJ", records.get(0).getKeyFields().get("sf_Id"));
    }

    // ===================== CDC: Include/Exclude Event =====================

    @Test
    void testCdcExcludeEvent() throws Exception {
        SalesforcePayloadStrategy noEventStrategy = new SalesforcePayloadStrategy();
        noEventStrategy.configure("sf_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        noEventStrategy.configureAdvanced(Collections.emptySet(), false, "", false);

        Map<String, Object> payload = buildCdcPayload("Account", "CREATE",
                "001D000000KnaXjIAJ", Map.of("Name", "Acme"));

        List<RoutedRecord> records = noEventStrategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertNull(result.get("ChangeEventHeader"));
        assertEquals("Acme", result.get("Name"));
        assertEquals(false, result.get("__deleted"));
    }

    @Test
    void testCdcIncludeEvent() throws Exception {
        Map<String, Object> payload = buildCdcPayload("Account", "CREATE",
                "001D000000KnaXjIAJ", Map.of("Name", "Acme"));

        List<RoutedRecord> records = strategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertNotNull(result.get("ChangeEventHeader"));
    }

    // ===================== PUSHTOPIC EVENTS =====================

    @Test
    void testPushTopicCreated() throws Exception {
        Map<String, Object> payload = buildPushTopicPayload(
                "/topic/AccountStream", "created",
                Map.of("Id", "001xx000001", "Name", "Acme", "Phone", "555-1234"));

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("sf_accountstream_events", records.get(0).getTopic());
        assertEquals("AccountStream.created", records.get(0).getEventType());
        assertEquals("001xx000001", records.get(0).getKeyFields().get("Id"));

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals("001xx000001", result.get("Id"));
        assertEquals("Acme", result.get("Name"));
        assertEquals(false, result.get("__deleted"));
    }

    @Test
    void testPushTopicDeleted() throws Exception {
        Map<String, Object> payload = buildPushTopicPayload(
                "/topic/AccountStream", "deleted",
                Map.of("Id", "001xx000001"));

        List<RoutedRecord> records = strategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(true, result.get("__deleted"));
    }

    @Test
    void testPushTopicUndeleted() throws Exception {
        Map<String, Object> payload = buildPushTopicPayload(
                "/topic/AccountStream", "undeleted",
                Map.of("Id", "001xx000001", "Name", "Acme"));

        List<RoutedRecord> records = strategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(false, result.get("__deleted"));
    }

    @Test
    void testPushTopicFlatten() throws Exception {
        SalesforcePayloadStrategy ptFlatten = new SalesforcePayloadStrategy();
        ptFlatten.configure("sf_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        ptFlatten.configureAdvanced(Collections.emptySet(), true, "pt_", true);

        Map<String, Object> payload = buildPushTopicPayload(
                "/topic/AccountStream", "created",
                Map.of("Id", "001xx000001", "Name", "Acme"));

        List<RoutedRecord> records = ptFlatten.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals("001xx000001", result.get("pt_Id"));
        assertEquals("Acme", result.get("pt_Name"));
        // Key matches flattened field
        assertEquals("001xx000001", records.get(0).getKeyFields().get("pt_Id"));
    }

    // ===================== PLATFORM EVENTS =====================

    @Test
    void testPlatformEvent() throws Exception {
        Map<String, Object> eventPayload = new LinkedHashMap<>();
        eventPayload.put("Id", "e03xx000000001");
        eventPayload.put("CreatedDate", "2024-03-27T15:30:00Z");
        eventPayload.put("Printer_Model__c", "XZO-5");
        eventPayload.put("Ink_Percentage__c", 0.2);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("replayId", 2);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("payload", eventPayload);
        data.put("event", event);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", data);
        payload.put("channel", "/event/Low_Ink__e");

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("sf_low_ink__e_events", records.get(0).getTopic());
        assertEquals("Low_Ink__e.published", records.get(0).getEventType());
        assertEquals("e03xx000000001", records.get(0).getKeyFields().get("Id"));

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals("XZO-5", result.get("Printer_Model__c"));
        assertEquals(false, result.get("__deleted"));
    }

    @Test
    void testPlatformEventNeverDeleted() throws Exception {
        Map<String, Object> eventPayload = new LinkedHashMap<>();
        eventPayload.put("Id", "e03xx000000001");

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("payload", eventPayload);
        data.put("event", Map.of("replayId", 5));

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", data);
        payload.put("channel", "/event/Alert__e");

        List<RoutedRecord> records = strategy.route(payload);

        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertEquals(false, result.get("__deleted"));
    }

    // ===================== UNKNOWN EVENTS =====================

    @Test
    void testUnknownPayloadRoutesToDefault() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("someField", "someValue");

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("sf_unknown", records.get(0).getTopic());
    }

    @Test
    void testUnknownPayloadSkipBehavior() {
        SalesforcePayloadStrategy skipStrategy = new SalesforcePayloadStrategy();
        skipStrategy.configure("sf_", UnknownTypeBehavior.SKIP, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("someField", "someValue");

        assertTrue(skipStrategy.route(payload).isEmpty());
    }

    @Test
    void testUnknownPayloadFailBehavior() {
        SalesforcePayloadStrategy failStrategy = new SalesforcePayloadStrategy();
        failStrategy.configure("sf_", UnknownTypeBehavior.FAIL, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("someField", "someValue");

        assertThrows(PayloadRoutingException.class, () -> failStrategy.route(payload));
    }

    // ===================== VALIDATION =====================

    @Test
    void testCdcMissingEntityName() {
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("changeType", "CREATE");
        // No entityName

        Map<String, Object> cdcPayload = new LinkedHashMap<>();
        cdcPayload.put("ChangeEventHeader", header);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("payload", cdcPayload);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", data);

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // ===================== FACTORY =====================

    @Test
    void testCreateStrategySalesforce() {
        PayloadRoutingStrategy s = PayloadRouter.createStrategy("salesforce");
        assertNotNull(s);
        assertTrue(s instanceof SalesforcePayloadStrategy);
    }

    @Test
    void testCreateStrategySalesforceCaseInsensitive() {
        PayloadRoutingStrategy s = PayloadRouter.createStrategy("SALESFORCE");
        assertNotNull(s);
    }

    // ===================== TOPIC PREFIX =====================

    @Test
    void testEmptyPrefix() throws Exception {
        SalesforcePayloadStrategy noPrefixStrategy = new SalesforcePayloadStrategy();
        noPrefixStrategy.configure("", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = buildCdcPayload("Account", "CREATE",
                "001xx", Map.of("Name", "Test"));

        assertEquals("account_events", noPrefixStrategy.route(payload).get(0).getTopic());
    }

    @Test
    void testCustomPrefix() throws Exception {
        SalesforcePayloadStrategy customStrategy = new SalesforcePayloadStrategy();
        customStrategy.configure("prod_sf_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = buildCdcPayload("Account", "CREATE",
                "001xx", Map.of("Name", "Test"));

        assertEquals("prod_sf_account_events", customStrategy.route(payload).get(0).getTopic());
    }

    // ===================== HELPERS =====================

    private Map<String, Object> buildCdcPayload(String entityName, String changeType,
                                                 String recordId, Map<String, Object> fields) {
        Map<String, Object> header = new LinkedHashMap<>();
        header.put("entityName", entityName);
        header.put("changeType", changeType);
        header.put("commitTimestamp", 1501010206653L);
        header.put("commitUser", "005D0000001cSZs");
        header.put("transactionKey", "tx-001");
        if (recordId != null) {
            header.put("recordIds", Arrays.asList(recordId));
        } else {
            header.put("recordIds", Collections.emptyList());
        }
        header.put("nulledFields", Collections.emptyList());
        header.put("changedFields", Collections.emptyList());

        Map<String, Object> cdcPayload = new LinkedHashMap<>();
        cdcPayload.put("ChangeEventHeader", header);
        if (recordId != null) {
            cdcPayload.put("Id", recordId);
        }
        cdcPayload.putAll(fields);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("replayId", 6);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("schema", "testSchema123");
        data.put("payload", cdcPayload);
        data.put("event", event);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", data);
        payload.put("channel", "/data/" + entityName + "ChangeEvent");

        return payload;
    }

    private Map<String, Object> buildPushTopicPayload(String channel, String eventType,
                                                       Map<String, Object> sobjectFields) {
        Map<String, Object> sobject = new LinkedHashMap<>(sobjectFields);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("createdDate", "2024-03-27T16:40:08Z");
        event.put("replayId", 13);
        event.put("type", eventType);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("event", event);
        data.put("sobject", sobject);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", data);
        payload.put("channel", channel);

        return payload;
    }
}