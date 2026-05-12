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

| Config Value | Source | Generated Topic |
|---|---|---|
| `ticket.tags` | `detail.tags[]` | `{prefix}ticket_tags` |
| `ticket.custom_fields` | `detail.custom_fields[]` | `{prefix}ticket_custom_fields` |
| `ticket.comments` | `event.comment` | `{prefix}ticket_comments` |
| `ticket.collaborators` | `detail.collaborator_ids[]` | `{prefix}ticket_collaborators` |
| `ticket.followers` | `detail.follower_ids[]` | `{prefix}ticket_followers` |
| `organization.tags` | `detail.tags[]` | `{prefix}organization_tags` |

## Example Configuration

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=zendesk
camel.source.payload.router.topic.prefix=zendesk_
camel.source.payload.router.fanout.fields=ticket.tags,ticket.custom_fields,ticket.comments
camel.source.payload.router.flatten.detail=true
camel.source.payload.router.include.event=true
```
