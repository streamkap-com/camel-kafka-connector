package org.apache.camel.kafkaconnector.nettyhttp.routing.zendesk;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.kafkaconnector.nettyhttp.routing.PayloadRoutingException;
import org.apache.camel.kafkaconnector.nettyhttp.routing.RoutedRecord;
import org.apache.camel.kafkaconnector.nettyhttp.routing.UnknownTypeBehavior;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ZendeskPayloadStrategyTest {

    private ZendeskPayloadStrategy strategy;
    private ZendeskPayloadStrategy strategyWithFanout;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        strategy = new ZendeskPayloadStrategy();
        strategy.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");

        strategyWithFanout = new ZendeskPayloadStrategy();
        strategyWithFanout.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        Set<String> fanoutFields = new HashSet<>();
        fanoutFields.add("ticket.tags");
        fanoutFields.add("ticket.custom_fields");
        fanoutFields.add("ticket.comments");
        fanoutFields.add("ticket.collaborators");
        fanoutFields.add("ticket.followers");
        fanoutFields.add("organization.tags");
        strategyWithFanout.configureAdvanced(fanoutFields, false, "detail_", true);
    }

    // ===================== TICKET EVENTS =====================

    @Test
    void testTicketCreatedNoFanout() throws Exception {
        Map<String, Object> payload = buildTicketPayload("zen:event-type:ticket.created");
        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());

        // Key is struct with ticket_id
        assertEquals(987654, records.get(0).getKeyFields().get("ticket_id"));
    }

    @Test
    void testTicketCreatedWithFanout() throws Exception {
        Map<String, Object> payload = buildTicketPayload("zen:event-type:ticket.created");
        List<RoutedRecord> records = strategyWithFanout.route(payload);

        // 1 main + 2 tags + 1 custom_field = 4
        assertEquals(4, records.size());
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());

        // Tag key: {ticket_id, value}
        assertEquals("zendesk_ticket_tags", records.get(1).getTopic());
        assertEquals(987654, records.get(1).getKeyFields().get("ticket_id"));
        assertEquals("urgent", records.get(1).getKeyFields().get("value"));

        assertEquals("zendesk_ticket_tags", records.get(2).getTopic());
        assertEquals("billing", records.get(2).getKeyFields().get("value"));

        // Custom field key: {ticket_id, id}
        assertEquals("zendesk_ticket_custom_fields", records.get(3).getTopic());
        assertEquals(987654, records.get(3).getKeyFields().get("ticket_id"));
        assertEquals(123, records.get(3).getKeyFields().get("id"));
    }

    @Test
    void testDomainScopedFanoutOnlyTicketTags() throws Exception {
        ZendeskPayloadStrategy selectiveFanout = new ZendeskPayloadStrategy();
        selectiveFanout.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        selectiveFanout.configureAdvanced(
                new HashSet<>(Collections.singletonList("ticket.tags")),
                false, "detail_", true);

        // Ticket with tags should fan out
        List<RoutedRecord> ticketRecords = selectiveFanout.route(buildTicketPayload("zen:event-type:ticket.created"));
        assertEquals(3, ticketRecords.size()); // 1 main + 2 tags

        // Organization with tags should NOT fan out
        Map<String, Object> orgPayload = new LinkedHashMap<>();
        orgPayload.put("type", "zen:event-type:organization.created");
        Map<String, Object> orgDetail = new LinkedHashMap<>();
        orgDetail.put("id", 111222);
        orgDetail.put("tags", Arrays.asList("enterprise", "vip"));
        orgPayload.put("detail", orgDetail);

        List<RoutedRecord> orgRecords = selectiveFanout.route(orgPayload);
        assertEquals(1, orgRecords.size());
    }

    @Test
    void testTicketCommentFanoutKeyIsCommentId() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.comment_added");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 987654);
        detail.put("subject", "Help");
        payload.put("detail", detail);

        Map<String, Object> comment = new LinkedHashMap<>();
        comment.put("id", 555666);
        comment.put("body", "Fixed it");
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("comment", comment);
        payload.put("event", event);

        List<RoutedRecord> records = strategyWithFanout.route(payload);

        assertEquals(2, records.size());
        // Comment key is just {id} — the comment's own ID
        assertEquals("zendesk_ticket_comments", records.get(1).getTopic());
        assertEquals(555666, records.get(1).getKeyFields().get("id"));
        assertEquals(1, records.get(1).getKeyFields().size());
    }

    @Test
    void testTicketCollaboratorsCompositeKey() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.updated");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 500);
        Map<String, Object> collab = new LinkedHashMap<>();
        collab.put("id", 10);
        collab.put("name", "Collab A");
        detail.put("collaborators", Arrays.asList(collab));
        Map<String, Object> follower = new LinkedHashMap<>();
        follower.put("id", 20);
        follower.put("name", "Follower B");
        detail.put("followers", Arrays.asList(follower));
        payload.put("detail", detail);

        List<RoutedRecord> records = strategyWithFanout.route(payload);

        assertEquals(3, records.size());
        // Collaborator key: {ticket_id: 500, id: 10}
        assertEquals(500, records.get(1).getKeyFields().get("ticket_id"));
        assertEquals(10, records.get(1).getKeyFields().get("id"));
        // Follower key: {ticket_id: 500, id: 20}
        assertEquals(500, records.get(2).getKeyFields().get("ticket_id"));
        assertEquals(20, records.get(2).getKeyFields().get("id"));
    }

    @Test
    void testTicketEventWithEmptyTags() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.updated");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 111);
        detail.put("tags", new ArrayList<>());
        payload.put("detail", detail);

        List<RoutedRecord> records = strategyWithFanout.route(payload);

        assertEquals(1, records.size());
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

    // ===================== USER EVENTS =====================

    @Test
    void testUserCreatedEvent() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.created");
        payload.put("account_id", 123456);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 6596848315901L);
        detail.put("email", "jane@example.com");
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_user_events", records.get(0).getTopic());
        assertEquals(6596848315901L, records.get(0).getKeyFields().get("user_id"));
    }

    @Test
    void testUserEventMissingDetailId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.updated");
        payload.put("detail", new LinkedHashMap<>());
        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // ===================== ORGANIZATION EVENTS =====================

    @Test
    void testOrganizationNoFanout() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:organization.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 111222);
        detail.put("tags", Arrays.asList("enterprise", "vip"));
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);
        assertEquals(1, records.size());
        assertEquals(111222, records.get(0).getKeyFields().get("organization_id"));
    }

    @Test
    void testOrganizationTagsFanoutCompositeKey() throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:organization.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 111222);
        detail.put("tags", Arrays.asList("enterprise", "vip"));
        payload.put("detail", detail);

        List<RoutedRecord> records = strategyWithFanout.route(payload);

        assertEquals(3, records.size());
        // Tag key: {organization_id, value}
        assertEquals(111222, records.get(1).getKeyFields().get("organization_id"));
        assertEquals("enterprise", records.get(1).getKeyFields().get("value"));
        assertEquals(111222, records.get(2).getKeyFields().get("organization_id"));
        assertEquals("vip", records.get(2).getKeyFields().get("value"));
    }

    @Test
    void testOrganizationEventMissingDetailId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:organization.deleted");
        payload.put("detail", new LinkedHashMap<>());
        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // ===================== ARTICLE EVENTS =====================

    @Test
    void testArticlePublishedEvent() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:article.published");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 888999);
        detail.put("brand_id", 111);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_article_events", records.get(0).getTopic());
        assertEquals(888999, records.get(0).getKeyFields().get("article_id"));
    }

    @Test
    void testArticleMissingDetailId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:article.created");
        payload.put("detail", new LinkedHashMap<>());
        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // ===================== COMMUNITY POST EVENTS =====================

    @Test
    void testCommunityPostEvent() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:community_post.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 777888);
        detail.put("topic_id", 333);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_community_post_events", records.get(0).getTopic());
        assertEquals(777888, records.get(0).getKeyFields().get("community_post_id"));
    }

    // ===================== MESSAGING EVENTS =====================

    @Test
    void testMessagingTicketEvent() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:messaging_ticket.message_added");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 555444);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_messaging_events", records.get(0).getTopic());
        assertEquals(555444, records.get(0).getKeyFields().get("messaging_ticket_id"));
    }

    // ===================== AGENT AVAILABILITY EVENTS =====================

    @Test
    void testAgentAvailabilityEvent() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:agent.status_changed");
        payload.put("account_id", 123456);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("agent_id", 99887766);
        detail.put("version", 3);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_agent_events", records.get(0).getTopic());
        assertEquals(99887766, records.get(0).getKeyFields().get("agent_id"));
    }

    @Test
    void testAgentEventFallsBackToId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:agent.channel_updated");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 11223344);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals(11223344, records.get(0).getKeyFields().get("agent_id"));
    }

    @Test
    void testAgentEventMissingIds() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:agent.status_changed");
        payload.put("detail", new LinkedHashMap<>());
        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    // ===================== OMNICHANNEL CONFIG EVENTS =====================

    @Test
    void testOmnichannelConfigEvent() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:omnichannel_config.feature_toggled");
        payload.put("account_id", 123456);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("feature", "skills_based_routing");
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_omnichannel_config_events", records.get(0).getTopic());
        assertEquals(123456, records.get(0).getKeyFields().get("account_id"));
    }

    @Test
    void testOmnichannelConfigNoAccountId() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:omnichannel_config.feature_toggled");

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertFalse(records.get(0).hasKey());
    }

    // ===================== MESSAGING METRICS EVENTS =====================

    @Test
    void testMessagingMetricsEvent() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:messaging_live_metrics.wait_time_changed");
        payload.put("account_id", 123456);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("metric", "avg_wait_time");
        detail.put("group_id", 77);
        payload.put("detail", detail);

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_messaging_metrics_events", records.get(0).getTopic());
        assertEquals(123456, records.get(0).getKeyFields().get("account_id"));
    }

    // ===================== DETAIL FLATTENING =====================

    @Test
    void testFlattenDetail() throws Exception {
        ZendeskPayloadStrategy flattenStrategy = new ZendeskPayloadStrategy();
        flattenStrategy.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        flattenStrategy.configureAdvanced(Collections.emptySet(), true, "detail_", true);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.created");
        payload.put("id", "uuid-event-id");
        payload.put("subject", "zen:ticket:987654");
        payload.put("account_id", 123456);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 987654);
        detail.put("subject", "Cannot login");
        detail.put("status", "new");
        payload.put("detail", detail);

        List<RoutedRecord> records = flattenStrategy.route(payload);
        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);

        assertEquals("uuid-event-id", result.get("id"));
        assertEquals("zen:ticket:987654", result.get("subject"));
        assertEquals(987654, result.get("detail_id"));
        assertEquals("Cannot login", result.get("detail_subject"));
        assertNull(result.get("detail"));
    }

    @Test
    void testFlattenDetailCustomPrefix() throws Exception {
        ZendeskPayloadStrategy flattenStrategy = new ZendeskPayloadStrategy();
        flattenStrategy.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        flattenStrategy.configureAdvanced(Collections.emptySet(), true, "d_", true);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 555);
        detail.put("name", "Jane");
        payload.put("detail", detail);

        List<RoutedRecord> records = flattenStrategy.route(payload);
        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);

        assertEquals(555, result.get("d_id"));
        assertEquals("Jane", result.get("d_name"));
    }

    @Test
    void testFlattenWithFanout() throws Exception {
        ZendeskPayloadStrategy bothStrategy = new ZendeskPayloadStrategy();
        bothStrategy.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        Set<String> fanout = new HashSet<>();
        fanout.add("ticket.tags");
        bothStrategy.configureAdvanced(fanout, true, "detail_", true);

        List<RoutedRecord> records = bothStrategy.route(buildTicketPayload("zen:event-type:ticket.created"));

        assertEquals(3, records.size()); // 1 main + 2 tags
        Map<String, Object> main = objectMapper.readValue(records.get(0).getPayload(), Map.class);
        assertNull(main.get("detail"));
        assertEquals(987654, main.get("detail_id"));
    }

    // ===================== EXCLUDE EVENT =====================

    @Test
    void testExcludeEventField() throws Exception {
        ZendeskPayloadStrategy noEventStrategy = new ZendeskPayloadStrategy();
        noEventStrategy.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        noEventStrategy.configureAdvanced(Collections.emptySet(), false, "detail_", false);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.comment_added");
        payload.put("account_id", 123456);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 987654);
        detail.put("status", "open");
        payload.put("detail", detail);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("comment", Map.of("id", 555, "body", "Hello"));
        payload.put("event", event);

        List<RoutedRecord> records = noEventStrategy.route(payload);
        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);

        // event field excluded
        assertNull(result.get("event"));
        // other fields still present
        assertNotNull(result.get("detail"));
        assertEquals(123456, result.get("account_id"));
    }

    @Test
    void testIncludeEventFieldDefault() throws Exception {
        // Default: includeEvent=true
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.updated");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 100);
        payload.put("detail", detail);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("previous_value", "open");
        event.put("current_value", "solved");
        payload.put("event", event);

        List<RoutedRecord> records = strategy.route(payload);
        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);

        // event field included by default
        assertNotNull(result.get("event"));
    }

    @Test
    void testFlattenWithExcludeEvent() throws Exception {
        ZendeskPayloadStrategy flatNoEvent = new ZendeskPayloadStrategy();
        flatNoEvent.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        flatNoEvent.configureAdvanced(Collections.emptySet(), true, "detail_", false);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:ticket.comment_added");
        payload.put("id", "uuid-123");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 987654);
        detail.put("status", "open");
        payload.put("detail", detail);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("comment", Map.of("id", 555, "body", "Hello"));
        payload.put("event", event);

        List<RoutedRecord> records = flatNoEvent.route(payload);
        Map<String, Object> result = objectMapper.readValue(records.get(0).getPayload(), Map.class);

        // Flattened detail, no event, no detail wrapper
        assertEquals(987654, result.get("detail_id"));
        assertEquals("open", result.get("detail_status"));
        assertNull(result.get("detail"));
        assertNull(result.get("event"));
        assertEquals("uuid-123", result.get("id"));
    }

    // ===================== UNKNOWN / EDGE CASES =====================

    @Test
    void testUnknownEventTypeRoutesToDefault() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:custom_object.created");
        payload.put("data", "some data");

        List<RoutedRecord> records = strategy.route(payload);

        assertEquals(1, records.size());
        assertEquals("zendesk_unknown", records.get(0).getTopic());
        assertFalse(records.get(0).hasKey());
    }

    @Test
    void testUnknownEventTypeSkipBehavior() {
        strategy.configure("zendesk_", UnknownTypeBehavior.SKIP, "unknown");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:custom_object.created");
        assertTrue(strategy.route(payload).isEmpty());
    }

    @Test
    void testUnknownEventTypeFailBehavior() {
        strategy.configure("zendesk_", UnknownTypeBehavior.FAIL, "unknown");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:custom_object.created");
        assertThrows(PayloadRoutingException.class, () -> strategy.route(payload));
    }

    @Test
    void testMissingTypeField() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("data", "no type");
        assertEquals("zendesk_unknown", strategy.route(payload).get(0).getTopic());
    }

    @Test
    void testNullTypeField() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", null);
        assertEquals("zendesk_unknown", strategy.route(payload).get(0).getTopic());
    }

    @Test
    void testSimplifiedTypeFormat() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "ticket.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 100);
        payload.put("detail", detail);
        assertEquals("zendesk_ticket_events", strategy.route(payload).get(0).getTopic());
    }

    @Test
    void testEmptyTopicPrefix() {
        strategy.configure("", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 1);
        payload.put("detail", detail);
        assertEquals("user_events", strategy.route(payload).get(0).getTopic());
    }

    @Test
    void testCustomTopicPrefix() {
        strategy.configure("prod_zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "zen:event-type:user.created");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", 1);
        payload.put("detail", detail);
        assertEquals("prod_zendesk_user_events", strategy.route(payload).get(0).getTopic());
    }

    // ===================== HELPERS =====================

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