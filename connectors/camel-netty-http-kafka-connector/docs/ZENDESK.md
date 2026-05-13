# Zendesk Configuration

Supported when `camel.source.payload.router.type=zendesk`.

## Event Domains and Topics

| Zendesk Domain | Example Event Type | Generated Topic |
|---------------|-------------------|-----------------|
| Tickets | `zen:event-type:ticket.created` | `{prefix}ticket_events` |
| Users | `zen:event-type:user.updated` | `{prefix}user_events` |
| Organizations | `zen:event-type:organization.deleted` | `{prefix}organization_events` |
| Articles | `zen:event-type:article.published` | `{prefix}article_events` |
| Community Posts | `zen:event-type:community_post.created` | `{prefix}community_post_events` |
| Messaging | `zen:event-type:messaging_ticket.message_added` | `{prefix}messaging_events` |
| Agent Availability | `zen:event-type:agent.status_changed` | `{prefix}agent_events` |
| Omnichannel Config | `zen:event-type:omnichannel_config.feature_toggled` | `{prefix}omnichannel_config_events` |
| Messaging Metrics | `zen:event-type:messaging_live_metrics.wait_time_changed` | `{prefix}messaging_metrics_events` |

## Payload Structure

Zendesk sends webhooks as HTTP POST with JSON body. The event type is in the `type` field of the body (not in HTTP headers).

```json
{
  "type": "zen:event-type:ticket.created",
  "id": "event-uuid-123",
  "account_id": 123456,
  "time": "2025-01-24T15:30:00Z",
  "subject": "zen:ticket:987654",
  "detail": {
    "id": 987654,
    "subject": "Cannot login",
    "status": "new",
    "tags": ["urgent", "billing"],
    "custom_fields": [{"id": 123, "value": "tier1"}]
  },
  "event": {
    "comment": {"id": 555, "body": "We are looking into it"}
  }
}
```

Each record gets these synthetic fields:
- `__deleted` — `true` for events ending in `deleted` or `removed` (excluding `soft_deleted` and `undeleted`)
- `__op` — Kafka header: `c` (created), `d` (deleted/removed), `u` (everything else)

## Message Keys

| Record Type | Key Schema |
|-------------|-----------|
| Main events (ticket, user, org, etc.) | `{detail_id: <detail.id>}` |
| Agent events | `{detail_agent_id: <detail.agent_id>}` |
| Account-level events | `{account_id: <account_id>}` |
| Fan-out: tags | `{detail_id: <parent_id>, value: <tag>}` |
| Fan-out: custom_fields | `{detail_id: <parent_id>, id: <field_id>}` |
| Fan-out: comments | `{id: <comment_id>}` |
| Fan-out: collaborators/followers | `{detail_id: <parent_id>, id: <entity_id>}` |

All key fields are also present in the value with the same name, enabling sink connectors to read from either key or value.

## Fan-Out Fields

| Config Value | Source Field | Generated Topic |
|---|---|---|
| `ticket.tags` | `detail.tags[]` | `{prefix}ticket_tags` |
| `ticket.custom_fields` | `detail.custom_fields[]` | `{prefix}ticket_custom_fields` |
| `ticket.comments` | `event.comment` | `{prefix}ticket_comments` |
| `ticket.collaborators` | `detail.collaborators[]` | `{prefix}ticket_collaborators` |
| `ticket.followers` | `detail.followers[]` | `{prefix}ticket_followers` |
| `organization.tags` | `detail.tags[]` | `{prefix}organization_tags` |

Fan-out records include context fields for correlation:
- `_ctx_event_id` — webhook event UUID
- `_ctx_ticket_subject` — ticket subject (for ticket fan-out records)

## Setting Up Zendesk Webhooks

### Step 1: Create a Webhook Destination

1. In Zendesk Admin Center, go to **Apps and integrations** > **Webhooks** > **Webhooks**
2. Click **Create webhook**
3. Fill in:
   - **Name**: `Kafka Connector`
   - **Endpoint URL**: `https://your-connector.example.com?api_key=your-connector-api-key`
   - **Request method**: POST
   - **Request format**: JSON
4. Click **Create webhook**

> **Note**: Like Shopify, Zendesk does not support custom headers on webhook deliveries. Pass the API key as a query parameter in the endpoint URL.

### Step 2: Create a Trigger or Automation

Zendesk uses **Triggers** (for immediate events) and **Automations** (for time-based events) to fire webhooks.

**For ticket events:**

1. Go to **Objects and rules** > **Business rules** > **Triggers**
2. Click **Create trigger**
3. Set conditions (e.g., "Ticket is created", "Ticket is updated")
4. Under **Actions**, select **Notify active webhook** > select your `Kafka Connector` webhook
5. Set the JSON body to the webhook payload format shown above
6. Save

**For the recommended approach**, use the **Zendesk Events API** (Event Subscriptions) which sends all events automatically in the `zen:event-type:domain.event` format without needing individual triggers.

### Step 3: Configure API Scopes (for Event Subscriptions)

If using Event Subscriptions:
1. Register your app in the Zendesk Developer Portal
2. Configure OAuth scopes: `read`, `tickets:read`, `users:read`, `organizations:read`
3. Subscribe to event types via the Events API

### Step 4: Configure the Connector

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=zendesk
camel.source.payload.router.topic.prefix=zendesk_
camel.source.payload.router.fanout.fields=ticket.tags,ticket.custom_fields,ticket.comments
camel.source.payload.router.flatten.detail=true
camel.source.payload.router.include.event=true

camel.source.dlq.enabled=true
camel.source.dlq.topic=zendesk_dlq
```

### Testing with curl

```bash
curl -X POST "http://localhost:8083/webhook?api_key=your-connector-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "zen:event-type:ticket.created",
    "id": "evt-test-123",
    "account_id": 123456,
    "detail": {
      "id": 987654,
      "subject": "Test ticket",
      "status": "new",
      "tags": ["test"]
    },
    "event": {}
  }'
```

## Snapshot

Snapshot is **not yet supported** for Zendesk. The Zendesk Incremental Export API is planned for a future release. Signal-triggered snapshots will not work until a `ZendeskChunkReader` is implemented.

For initial data loads, use the Zendesk API directly to export data, then produce records to Kafka manually.
