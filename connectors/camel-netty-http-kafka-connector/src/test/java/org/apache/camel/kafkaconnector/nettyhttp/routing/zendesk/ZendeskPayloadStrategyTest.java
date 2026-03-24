package org.apache.camel.kafkaconnector.nettyhttp.routing.zendesk;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Arrays;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ZendeskPayloadStrategyTest {

    private ZendeskPayloadStrategy strategy;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        strategy = new ZendeskPayloadStrategy();
        strategy.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
    }

    // --- Ticket Events ---

    @Test
    void testTicketCreatedEvent() throws Exception {
        Map<String, Object> payload = buildTicketPayload("zen:event-type:ticket.created");

        List<RoutedRecord> records = strategy.route(payload);

        // 1 main + 2 tags + 1 custom_field = 4 records
        assertEquals(4, records.size());

        // Main ticket event
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());
        assertEquals("zen:event-type:ticket.created", records.get(0).getEventType());

        // Verify main record contains full payload
        Map<String, Object> mainPayload = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertNotNull(mainPayload.get("detail"));

        // Tag records
        assertEquals("zendesk_ticket_tags", records.get(1).getTopic());
        Map<String, Object> tag1 = objectMapper.readValue(records.get(1).getPayload(), Map.class);
        assertEquals("urgent", tag1.get("value"));
        assertEquals(987654, tag1.get("_ctx_ticket_id"));

        assertEquals("zendesk_ticket_tags", records.get(2).getTopic());
        Map<String, Object> tag2 = objectMapper.readValue(records.get(2).getPayload(), Map.class);
        assertEquals("billing", tag2.get("value"));
        assertEquals(987654, tag2.get("_ctx_ticket_id"));

        // Custom field record
        assertEquals("zendesk_ticket_custom_fields", records.get(3).getTopic());
        Map<String, Object> cf = objectMapper.readValue(records.get(3).getPayload(), Map.class);
        assertEquals(123, cf.get("id"));
        assertEquals("tier1", cf.get("value"));
        assertEquals(987654, cf.get("_ctx_ticket_id"));
    }

    @Test
    void testTicketCommentAddedEvent() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.comment_added");
        payload.put("account_id", 123456);

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 987654);
        detail.put("subject", "Help needed");
        payload.put("detail", detail);

        Map<String, Object> comment = new LinkedHashMap<>();
        comment.put("id", 555666);
        comment.put("body", "I've reset your password.");
        comment.put("public", true);
        comment.put("author_id", 334455);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("comment", comment);
        payload.put("event", event);

        List<RoutedRecord> records = strategy.route(payload);

        // 1 main + 1 comment = 2 records
        assertEquals(2, records.size());

        assertEquals("zendesk_ticket_events", records.get(0).getTopic());

        assertEquals("zendesk_ticket_comments", records.get(1).getTopic());
        Map<String, Object> commentRecord = objectMapper.readValue(records.get(1).getPayload(), Map.class);
        assertEquals(555666, commentRecord.get("id"));
        assertEquals("I've reset your password.", commentRecord.get("body"));
        assertEquals(987654, commentRecord.get("_ctx_ticket_id"));
        assertEquals("Help needed", commentRecord.get("_ctx_ticket_subject"));
    }

    @Test
    void testTicketEventWithEmptyTags() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.updated");

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 111);
        detail.put("tags", new ArrayList<>());
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        // Only main record, no tag splits
        assertEquals(1, records.size());
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());
    }

    @Test
    void testTicketEventMissingDetailId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.created");
        payload.put("detail", new LinkedHashMap<>());

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    @Test
    void testTicketEventMissingDetail() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.created");

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // --- User Events ---

    @Test
    void testUserCreatedEvent() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.created");
        payload.put("account_id", 123456);

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 6596848315901L);
        detail.put("email", "jane@example.com");
        detail.put("name", "Jane Smith");
        detail.put("role", "end-user");
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        // User events produce a single record
        assertEquals(1, records.size());
        assertEquals("zendesk_user_events", records.get(0).getTopic());
        assertEquals("zen:event-type:user.created", records.get(0).getEventType());

        Map<String, Object> recordPayload = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        Map<String, Object> recordDetail = (Map<String, Object>) recordPayload.get("detail");
        assertEquals("jane@example.com", recordDetail.get("email"));
    }

    @Test
    void testUserEventMissingDetailId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.updated");
        payload.put("detail", new LinkedHashMap<>());

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // --- Organization Events ---

    @Test
    void testOrganizationCreatedWithTags() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:organization.created");

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 111222);
        detail.put("name", "Acme Corporation");
        detail.put("tags", Arrays.asList("enterprise", "vip"));
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        // 1 main + 2 tags = 3
        assertEquals(3, records.size());
        assertEquals("zendesk_organization_events", records.get(0).getTopic());

        assertEquals("zendesk_organization_tags", records.get(1).getTopic());
        Map<String, Object> tag1 = objectMapper.readValue(records.get(1).getPayload(), Map.class);
        assertEquals("enterprise", tag1.get("value"));
        assertEquals(111222, tag1.get("_ctx_organization_id"));

        assertEquals("zendesk_organization_tags", records.get(2).getTopic());
        Map<String, Object> tag2 = objectMapper.readValue(records.get(2).getPayload(), Map.class);
        assertEquals("vip", tag2.get("value"));
    }

    @Test
    void testOrganizationEventMissingDetailId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:organization.deleted");
        payload.put("detail", new LinkedHashMap<>());

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // --- Unknown Event Types ---

    @Test
    void testUnknownEventTypeRoutesToDefault() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:article.published");
        payload.put("data", "some data");

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_unknown", records.get(0).getTopic());
    }

    @Test
    void testUnknownEventTypeSkipBehavior() {
        strategy.configure("zendesk_", UnknownTypeBehavior.SKIP, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:article.published");

        List<RoutedRecord> records = strategy.route(payload);

        assertTrue(records.isEmpty());
    }

    @Test
    void testUnknownEventTypeFailBehavior() {
        strategy.configure("zendesk_", UnknownTypeBehavior.FAIL, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:article.published");

        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    @Test
    void testMissingTypeField() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", "no type field");

        List<RoutedRecord> records = strategy.route(payload);

        // Routes to default topic
        assertEquals(1, records.size());
        assertEquals("zendesk_unknown", records.get(0).getTopic());
    }

    @Test
    void testNullTypeField() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", null);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_unknown", records.get(0).getTopic());
    }

    // --- Simplified Type Format ---

    @Test
    void testSimplifiedTypeFormat() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "ticket.created");

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 100);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());
    }

    // --- Topic Prefix ---

    @Test
    void testEmptyTopicPrefix() throws Exception {
        strategy.configure("", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 1);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("user_events", records.get(0).getTopic());
    }

    @Test
    void testCustomTopicPrefix() throws Exception {
        strategy.configure("prod_zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 1);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals("prod_zendesk_user_events", records.get(0).getTopic());
    }

    // --- Ticket with collaborators and followers ---

    @Test
    @SuppressWarnings("unchecked")
    void testTicketWithCollaboratorsAndFollowers() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.updated");

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 500);

        Map<String, Object> collab1 = new LinkedHashMap<>();
        collab1.put("id", 10);
        collab1.put("name", "Collaborator A");
        detail.put("collaborators", Arrays.asList(collab1));

        Map<String, Object> follower1 = new LinkedHashMap<>();
        follower1.put("id", 20);
        follower1.put("name", "Follower B");
        detail.put("followers", Arrays.asList(follower1));

        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        // 1 main + 1 collaborator + 1 follower = 3
        assertEquals(3, records.size());
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());

        assertEquals("zendesk_ticket_collaborators", records.get(1).getTopic());
        Map<String, Object> collabRecord = objectMapper.readValue(records.get(1).getPayload(), Map.class);
        assertEquals(10, collabRecord.get("id"));
        assertEquals(500, collabRecord.get("_ctx_ticket_id"));

        assertEquals("zendesk_ticket_followers", records.get(2).getTopic());
        Map<String, Object> followerRecord = objectMapper.readValue(records.get(2).getPayload(), Map.class);
        assertEquals(20, followerRecord.get("id"));
        assertEquals(500, followerRecord.get("_ctx_ticket_id"));
    }

    // --- Helpers ---

    private Map<String, Object> buildTicketPayload(String eventType) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", eventType);
        payload.put("account_id", 123456);
        payload.put("id", "uuid-here");
        payload.put("time", "2025-01-24T15:30:00Z");
        payload.put("subject", "zen:ticket:987654");

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 987654);
        detail.put("subject", "Cannot login");
        detail.put("status", "new");
        detail.put("tags", Arrays.asList("urgent", "billing"));

        Map<String, Object> customField = new LinkedHashMap<>();
        customField.put("id", 123);
        customField.put("value", "tier1");
        detail.put("custom_fields", Arrays.asList(customField));

        payload.put("detail", detail);
        payload.put("event", new LinkedHashMap<>());

        return payload;
    }
}
