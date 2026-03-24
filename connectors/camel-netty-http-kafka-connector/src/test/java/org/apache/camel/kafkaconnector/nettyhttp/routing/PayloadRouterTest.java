package org.apache.camel.kafkaconnector.nettyhttp.routing;

import java.util.List;

import org.apache.camel.kafkaconnector.nettyhttp.routing.zendesk.ZendeskPayloadStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for PayloadRouter: tests the full pipeline from raw JSON string
 * through strategy selection, JSON parsing, routing, and fan-out.
 */
public class PayloadRouterTest {

    private PayloadRouter router;

    @BeforeEach
    void setUp() {
        PayloadRoutingStrategy strategy = PayloadRouter.createStrategy("zendesk");
        strategy.configure("zendesk_", UnknownTypeBehavior.DEFAULT_TOPIC, "unknown");
        router = new PayloadRouter(strategy);
    }

    // --- Full JSON Pipeline Tests ---

    @Test
    void testFullTicketCreatedPipeline() {
        String json = "{"
                + "\"type\":\"zen:event-type:ticket.created\","
                + "\"account_id\":123456,"
                + "\"detail\":{"
                +   "\"id\":987654,"
                +   "\"subject\":\"Cannot login\","
                +   "\"status\":\"new\","
                +   "\"tags\":[\"urgent\",\"billing\"],"
                +   "\"custom_fields\":[{\"id\":123,\"value\":\"tier1\"}]"
                + "},"
                + "\"event\":{}"
                + "}";

        List<RoutedRecord> records = router.route(json);

        // 1 main + 2 tags + 1 custom_field = 4
        assertEquals(4, records.size());
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());
        assertEquals("zendesk_ticket_tags", records.get(1).getTopic());
        assertEquals("zendesk_ticket_tags", records.get(2).getTopic());
        assertEquals("zendesk_ticket_custom_fields", records.get(3).getTopic());

        // All records should have valid JSON payloads
        for (RoutedRecord record : records) {
            assertNotNull(record.getPayload());
            assertTrue(record.getPayload().startsWith("{"));
            assertEquals("zen:event-type:ticket.created", record.getEventType());
        }
    }

    @Test
    void testFullUserCreatedPipeline() {
        String json = "{"
                + "\"type\":\"zen:event-type:user.created\","
                + "\"account_id\":123456,"
                + "\"detail\":{"
                +   "\"id\":6596848315901,"
                +   "\"email\":\"jane@example.com\","
                +   "\"name\":\"Jane Smith\""
                + "}"
                + "}";

        List<RoutedRecord> records = router.route(json);

        assertEquals(1, records.size());
        assertEquals("zendesk_user_events", records.get(0).getTopic());
        assertTrue(records.get(0).getPayload().contains("jane@example.com"));
    }

    @Test
    void testFullOrganizationWithTagsPipeline() {
        String json = "{"
                + "\"type\":\"zen:event-type:organization.created\","
                + "\"detail\":{"
                +   "\"id\":111222,"
                +   "\"name\":\"Acme Corp\","
                +   "\"tags\":[\"enterprise\",\"vip\"]"
                + "}"
                + "}";

        List<RoutedRecord> records = router.route(json);

        assertEquals(3, records.size());
        assertEquals("zendesk_organization_events", records.get(0).getTopic());
        assertEquals("zendesk_organization_tags", records.get(1).getTopic());
        assertEquals("zendesk_organization_tags", records.get(2).getTopic());
    }

    @Test
    void testFullCommentAddedPipeline() {
        String json = "{"
                + "\"type\":\"zen:event-type:ticket.comment_added\","
                + "\"detail\":{"
                +   "\"id\":987654,"
                +   "\"subject\":\"Help needed\""
                + "},"
                + "\"event\":{"
                +   "\"comment\":{"
                +     "\"id\":555666,"
                +     "\"body\":\"We are on it\","
                +     "\"author_id\":334455"
                +   "}"
                + "}"
                + "}";

        List<RoutedRecord> records = router.route(json);

        assertEquals(2, records.size());
        assertEquals("zendesk_ticket_events", records.get(0).getTopic());
        assertEquals("zendesk_ticket_comments", records.get(1).getTopic());
        assertTrue(records.get(1).getPayload().contains("We are on it"));
        assertTrue(records.get(1).getPayload().contains("_ctx_ticket_id"));
        assertTrue(records.get(1).getPayload().contains("_ctx_ticket_subject"));
    }

    // --- Error Handling ---

    @Test
    void testInvalidJson() {
        assertThrows(PayloadRoutingException.class, () -> router.route("not valid json"));
    }

    @Test
    void testEmptyPayload() {
        List<RoutedRecord> records = router.route("");
        assertTrue(records.isEmpty());
    }

    @Test
    void testNullPayload() {
        List<RoutedRecord> records = router.route(null);
        assertTrue(records.isEmpty());
    }

    @Test
    void testWhitespaceOnlyPayload() {
        List<RoutedRecord> records = router.route("   ");
        assertTrue(records.isEmpty());
    }

    // --- Strategy Factory ---

    @Test
    void testCreateStrategyZendesk() {
        PayloadRoutingStrategy strategy = PayloadRouter.createStrategy("zendesk");
        assertNotNull(strategy);
        assertTrue(strategy instanceof ZendeskPayloadStrategy);
    }

    @Test
    void testCreateStrategyCaseInsensitive() {
        PayloadRoutingStrategy strategy = PayloadRouter.createStrategy("ZENDESK");
        assertNotNull(strategy);
        assertTrue(strategy instanceof ZendeskPayloadStrategy);
    }

    @Test
    void testCreateStrategyUnknownType() {
        assertThrows(PayloadRoutingException.class, () -> PayloadRouter.createStrategy("salesforce"));
    }

    @Test
    void testCreateStrategyNullType() {
        assertThrows(PayloadRoutingException.class, () -> PayloadRouter.createStrategy(null));
    }

    @Test
    void testCreateStrategyEmptyType() {
        assertThrows(PayloadRoutingException.class, () -> PayloadRouter.createStrategy(""));
    }

    // --- Unknown Type via Router ---

    @Test
    void testUnknownEventTypeViaRouter() {
        String json = "{\"type\":\"zen:event-type:article.published\",\"data\":\"test\"}";

        List<RoutedRecord> records = router.route(json);

        assertEquals(1, records.size());
        assertEquals("zendesk_unknown", records.get(0).getTopic());
    }

    @Test
    void testSkipBehaviorViaRouter() {
        PayloadRoutingStrategy strategy = PayloadRouter.createStrategy("zendesk");
        strategy.configure("zendesk_", UnknownTypeBehavior.SKIP, "unknown");
        PayloadRouter skipRouter = new PayloadRouter(strategy);

        String json = "{\"type\":\"zen:event-type:article.published\",\"data\":\"test\"}";

        List<RoutedRecord> records = skipRouter.route(json);

        assertTrue(records.isEmpty());
    }
}
