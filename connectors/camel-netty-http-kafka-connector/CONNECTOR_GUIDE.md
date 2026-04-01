# Camel Netty HTTP Kafka Source Connector — Configuration Guide

## Overview

This connector receives webhook HTTP POST events from external applications (Zendesk, Salesforce, etc.) and routes them to Kafka topics. It provides:

- **Payload routing** — Automatic topic routing based on event type/domain
- **Fan-out** — Split nested arrays (tags, custom fields) into separate topics
- **Detail flattening** — Promote nested fields to top-level for easy sink consumption
- **Struct message keys** — Automatic key extraction for partitioning and upsert
- **Delete detection** — `__deleted` field for CDC-style delete handling
- **Source DLQ** — Dead letter queue for errored records
- **Snapshot** — Debezium-style initial and incremental snapshots with parallel execution

---

## Table of Contents

1. [Payload Routing](#1-payload-routing)
2. [Zendesk Configuration](#2-zendesk-configuration)
3. [Salesforce Configuration](#3-salesforce-configuration)
4. [Fan-out Configuration](#4-fan-out-configuration)
5. [Detail Flattening](#5-detail-flattening)
6. [Event Field Control](#6-event-field-control)
7. [Message Keys](#7-message-keys)
8. [Delete Detection](#8-delete-detection)
9. [Source Dead Letter Queue](#9-source-dead-letter-queue)
10. [Snapshot](#10-snapshot)
11. [Full Configuration Reference](#11-full-configuration-reference)
12. [Example Configurations](#12-example-configurations)

---

## 1. Payload Routing

The connector inspects incoming JSON payloads to determine the target Kafka topic and message key.

### Base Config

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.payload.router.enabled` | Boolean | `false` | Enable payload-based routing |
| `camel.source.payload.router.type` | String | — | Provider type: `zendesk`, `salesforce` |
| `camel.source.payload.router.topic.prefix` | String | `""` | Prefix for all generated topic names |
| `camel.source.payload.router.unknown.type.behavior` | String | `DEFAULT_TOPIC` | How to handle unrecognized events: `DEFAULT_TOPIC`, `SKIP`, `FAIL` |
| `camel.source.payload.router.unknown.type.default.topic` | String | `unknown` | Topic for unknown events (when behavior = DEFAULT_TOPIC) |

---

## 2. Zendesk Configuration

Supported when `camel.source.payload.router.type=zendesk`.

### Event Domains and Topics

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

### Zendesk Payload Structure

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

### Zendesk Message Keys

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

---

## 3. Salesforce Configuration

Supported when `camel.source.payload.router.type=salesforce`.

### Supported Event Formats

#### Change Data Capture (CDC)

Detected by presence of `data.payload.ChangeEventHeader`.

| changeType | Topic | `__deleted` |
|-----------|-------|------------|
| `CREATE` | `{prefix}{entity}_events` | `false` |
| `UPDATE` | `{prefix}{entity}_events` | `false` |
| `DELETE` | `{prefix}{entity}_events` | `true` |
| `UNDELETE` | `{prefix}{entity}_events` | `false` |
| `GAP_CREATE` | `{prefix}gap_events` | `false` |
| `GAP_DELETE` | `{prefix}gap_events` | `true` |
| `GAP_OVERFLOW` | `{prefix}gap_events` | `false` |

#### PushTopic Events

Detected by presence of `data.sobject`.

| event.type | Topic |
|-----------|-------|
| `created` | `{prefix}{topicname}_events` |
| `updated` | `{prefix}{topicname}_events` |
| `deleted` | `{prefix}{topicname}_events` |
| `undeleted` | `{prefix}{topicname}_events` |

#### Platform Events

Detected by presence of `data.payload` without `ChangeEventHeader`.

| | Topic |
|---|-------|
| All | `{prefix}{eventname}_events` |

Platform events always have `__deleted = false`.

### Salesforce Message Keys

All Salesforce events use `{Id: <record_id>}` as key (or `{<prefix>Id: <record_id>}` when flattened with a prefix).

### Setting Up Salesforce to Send Events to the Connector


### Setting Up Salesforce to Send Events to the Connector

This guide walks through setting up Apex triggers to automatically send all record changes (create, update, delete, undelete) to the connector with **all fields included dynamically**.

#### Prerequisites

You need:
- A Salesforce org (Developer, Professional, Enterprise, or Unlimited edition)
- Admin access (System Administrator profile)
- Your connector's public URL (e.g., `https://your-connector.example.com`)

#### Step 1: Create an External Client App (for Authentication)

This provides OAuth credentials for the Apex callout and snapshot API.

1. **Setup** > Quick Find > search **"App Manager"**
2. Click **New External Client App** (or **New Connected App** on older orgs)
3. Fill in:
   - **Name**: `Kafka Connector`
   - **Contact Email**: your email
   - **Distribution State**: `Local`
4. Under **Enable OAuth**:
   - Check **Enable OAuth Settings**
   - **Callback URL**: `https://login.salesforce.com/services/oauth2/callback`
   - **Selected OAuth Scopes**: add `Manage user data via APIs (api)`, `Full access (full)`, `Perform requests at any time (refresh_token, offline_access)`
5. Under **Security**: uncheck `Require Proof Key for Code Exchange (PKCE)`
6. Click **Create** / **Save**
7. After saving, note the **Consumer Key** and **Consumer Secret** (you may need to click "Manage Consumer Details" to reveal the secret)

Note: New apps may take 2-10 minutes to activate.

#### Step 2: Create Named Credential (for Secure HTTP Callouts)

Named Credentials store your connector's URL and authentication headers securely.

**Create External Credential:**

1. **Setup** > Quick Find > search **"Named Credentials"**
2. Click **External Credentials** tab > **New**
3. Fill in:
   - **Label**: `Kafka Connector Webhook`
   - **Name**: `Kafka_Connector_Webhook`
   - **Authentication Protocol**: `Custom`
4. Save
5. Under **Principals** section > click **New**:
   - **Parameter Name**: `Default`
   - **Identity Type**: `Named Principal` (read-only)
   - **Sequence Number**: `1`
6. Save
7. Click on the **Default** principal > under **Authentication Parameters** > **Add**:
   - **Parameter 1 Name**: `x-api-key`
   - **Parameter 1 Value**: your connector's API key
8. Save

**Create Named Credential:**

1. Click **Named Credentials** tab > **New**
2. Fill in:
   - **Label**: `Kafka Connector`
   - **Name**: `Kafka_Connector`
   - **URL**: `https://your-connector.example.com` (your connector's public URL, no path)
   - **External Credential**: select `Kafka Connector Webhook`
3. Save

**Grant Permission to Use the Credential:**

1. **Setup** > Quick Find > search **"Permission Sets"** > **New**
   - **Label**: `Kafka Webhook Access`
   - Save
2. Inside the permission set, click **External Credential Principal Access**
3. Click **Edit** > move `Kafka_Connector_Webhook - Default` to **Enabled** > Save
4. Click **Manage Assignments** > **Add Assignment** > select your user > **Assign**
5. Log out and log back in to refresh the session

#### Step 3: Create the Webhook Sender Apex Class

This class dynamically queries **all fields** (standard + custom) for any object and sends them to the connector in CDC format.

1. **Setup** > Quick Find > search **"Apex Classes"** > **New**
2. Paste this code:

```apex
public class WebhookSender {

    @future(callout=true)
    public static void sendAsync(String recordId, String changeType, String objectName) {
        Map<String, Schema.SObjectField> fieldMap = Schema.getGlobalDescribe()
            .get(objectName).getDescribe().fields.getMap();

        List<String> fieldNames = new List<String>();
        for (String fieldName : fieldMap.keySet()) {
            Schema.DescribeFieldResult fieldDesc = fieldMap.get(fieldName).getDescribe();
            if (fieldDesc.isAccessible() && !fieldDesc.getName().contains('Address')) {
                fieldNames.add(fieldDesc.getName());
            }
        }

        String soql = 'SELECT ' + String.join(fieldNames, ', ')
                     + ' FROM ' + objectName
                     + ' WHERE Id = \'' + String.escapeSingleQuotes(recordId) + '\'';

        List<SObject> records = Database.query(soql);
        if (records.isEmpty()) {
            sendPayload(recordId, objectName, changeType, new Map<String, Object>{'Id' => recordId});
            return;
        }

        Map<String, Object> recordMap = new Map<String, Object>();
        SObject record = records[0];
        for (String fieldName : fieldNames) {
            Object val = record.get(fieldName);
            if (val != null) {
                recordMap.put(fieldName, val);
            }
        }

        sendPayload(recordId, objectName, changeType, recordMap);
    }

    @future(callout=true)
    public static void sendDeleteAsync(String recordId, String objectName, String recordJson) {
        Map<String, Object> recordMap = (Map<String, Object>) JSON.deserializeUntyped(recordJson);
        sendPayload(recordId, objectName, 'DELETE', recordMap);
    }

    private static void sendPayload(String recordId, String objectName,
                                     String changeType, Map<String, Object> recordFields) {
        Map<String, Object> header = new Map<String, Object>{
            'entityName' => objectName,
            'recordIds' => new List<String>{recordId},
            'changeType' => changeType,
            'commitTimestamp' => System.currentTimeMillis()
        };

        Map<String, Object> payload = new Map<String, Object>();
        payload.put('ChangeEventHeader', header);
        payload.putAll(recordFields);

        Map<String, Object> body = new Map<String, Object>{
            'data' => new Map<String, Object>{
                'payload' => payload,
                'event' => new Map<String, Object>{'replayId' => 0}
            },
            'channel' => '/data/' + objectName + 'ChangeEvent'
        };

        HttpRequest req = new HttpRequest();
        req.setEndpoint('callout:Kafka_Connector');
        req.setMethod('POST');
        req.setHeader('Content-Type', 'application/json');
        req.setBody(JSON.serialize(body));
        req.setTimeout(30000);

        Http http = new Http();
        try {
            HttpResponse res = http.send(req);
            if (res.getStatusCode() != 200) {
                System.debug(LoggingLevel.ERROR, 'Webhook failed: ' + res.getStatusCode() + ' ' + res.getBody());
            }
        } catch (Exception e) {
            System.debug(LoggingLevel.ERROR, 'Webhook error: ' + e.getMessage());
        }
    }
}
```

3. Click **Save**

**What this class does:**
- `sendAsync()` — Dynamically discovers ALL fields (standard + custom) for the object, queries the full record, and sends it as a CDC-formatted HTTP POST
- `sendDeleteAsync()` — Sends delete events using the record data from `Trigger.old` (since deleted records can't be re-queried)
- `sendPayload()` — Builds the CDC payload format and sends via the Named Credential (includes `x-api-key` header automatically)
- Uses `callout:Kafka_Connector` — references the Named Credential by API name, no hardcoded URLs or credentials

**Do not modify the payload structure.** The connector's Salesforce strategy relies on these exact field names to detect and route events:

| Field Path | Required Value | Purpose |
|-----------|---------------|---------|
| `data` | Object wrapper | Top-level container |
| `data.payload` | Object with record fields | Record data + header |
| `data.payload.ChangeEventHeader` | Object with metadata | Identifies event as Salesforce CDC |
| `data.payload.ChangeEventHeader.entityName` | Object name (e.g., `"Account"`) | Determines Kafka topic |
| `data.payload.ChangeEventHeader.changeType` | `CREATE`, `UPDATE`, `DELETE`, `UNDELETE` | Sets `__deleted` field |
| `data.payload.Id` | Record ID | Used as message key |
| `data.event.replayId` | Integer | Included in output for tracking |
| `channel` | `/data/{Object}ChangeEvent` | Used for event format detection |

Renaming any of these fields (e.g., `payload` to `body`, or `ChangeEventHeader` to `header`) will cause the connector to not recognize the event and route it to the unknown/default topic.

#### Step 4: Create Apex Triggers per Object

Create one trigger per Salesforce object you want to capture. The trigger calls the generic `WebhookSender` class.

**For Account:**

1. **Setup** > Quick Find > search **"Object Manager"**
2. Click **Account** > **Triggers** (left sidebar) > **New**
3. Paste:

```apex
trigger AccountToKafka on Account (after insert, after update, after delete, after undelete) {
    List<SObject> records;
    String changeType;

    if (Trigger.isInsert) {
        changeType = 'CREATE';
        records = Trigger.new;
    } else if (Trigger.isUpdate) {
        changeType = 'UPDATE';
        records = Trigger.new;
    } else if (Trigger.isDelete) {
        changeType = 'DELETE';
        records = Trigger.old;
    } else if (Trigger.isUndelete) {
        changeType = 'UNDELETE';
        records = Trigger.new;
    }

    for (SObject rec : records) {
        if (changeType == 'DELETE') {
            WebhookSender.sendDeleteAsync(rec.Id, 'Account', JSON.serialize(rec));
        } else {
            WebhookSender.sendAsync(rec.Id, changeType, 'Account');
        }
    }
}
```

4. Click **Save**

**For Contact:**

1. **Object Manager** > **Contact** > **Triggers** > **New**
2. Paste (same pattern, just change the object name):

```apex
trigger ContactToKafka on Contact (after insert, after update, after delete, after undelete) {
    List<SObject> records;
    String changeType;

    if (Trigger.isInsert) {
        changeType = 'CREATE';
        records = Trigger.new;
    } else if (Trigger.isUpdate) {
        changeType = 'UPDATE';
        records = Trigger.new;
    } else if (Trigger.isDelete) {
        changeType = 'DELETE';
        records = Trigger.old;
    } else if (Trigger.isUndelete) {
        changeType = 'UNDELETE';
        records = Trigger.new;
    }

    for (SObject rec : records) {
        if (changeType == 'DELETE') {
            WebhookSender.sendDeleteAsync(rec.Id, 'Contact', JSON.serialize(rec));
        } else {
            WebhookSender.sendAsync(rec.Id, changeType, 'Contact');
        }
    }
}
```

3. Click **Save**

**To add more objects** (Lead, Opportunity, Case, etc.), create a new trigger with the same pattern — only change the trigger name, object name in the `trigger ... on {Object}` line, and the `'Object'` string in the `WebhookSender` calls.

#### Step 5: Configure the Connector

```properties
# Core connector settings
camel.source.path.protocol=http
camel.source.path.host=0.0.0.0
camel.source.path.port=8080
camel.source.endpoint.sync=true

# Payload routing for Salesforce
camel.source.payload.router.enabled=true
camel.source.payload.router.type=salesforce
camel.source.payload.router.topic.prefix=sf_

# Flatten for upsert-friendly output
camel.source.payload.router.flatten.detail=true
camel.source.payload.router.flatten.detail.prefix=detail_
camel.source.payload.router.include.event=false

# Error handling
camel.source.dlq.enabled=true
camel.source.dlq.topic=sf_dlq
errors.tolerance=all
errors.log.enable=true

# Snapshot (optional, for initial data load)
camel.source.snapshot.mode=no_data
camel.source.snapshot.signal.topic=sf_snapshot_signals
camel.source.snapshot.salesforce.instance.url=https://myorg.my.salesforce.com
camel.source.snapshot.salesforce.auth.client.id=your_consumer_key
camel.source.snapshot.salesforce.auth.client.secret=your_consumer_secret
camel.source.snapshot.salesforce.auth.username=your_username
camel.source.snapshot.salesforce.auth.password=your_password_with_security_token
```

#### Important: URL Path and Topic Routing

The Apex callout endpoint must have **no URL path** — just the base URL via the Named Credential:

```apex
// Correct — no path, payload router determines topic from payload content
req.setEndpoint('callout:Kafka_Connector');

// Wrong — /webhook path conflicts with CamelDynamicTopicTransform
req.setEndpoint('callout:Kafka_Connector/webhook');
```

If your connector config includes `CamelDynamicTopicTransform` (which sets topic from the HTTP path), a path like `/webhook` overrides the payload router's topic. With no path, the payload router's topic (e.g., `sf_account_events`) is preserved through the transform chain.

#### Step 6: Test End-to-End

1. **Create an Account** in Salesforce:
   - Click **Accounts** tab > **New**
   - Enter Account Name: `Test Company`
   - Fill in other fields (Phone, Billing City, etc.)
   - Click **Save**

2. **Check Kafka** (within a few seconds):
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 --topic sf_account_events --from-beginning
   ```
   You should see a record with all Account fields, `changeType: CREATE`, and `__deleted: false`.

3. **Update the Account**:
   - Open the Account > edit the Name > Save
   - Check Kafka — should see `changeType: UPDATE` with all current field values

4. **Delete the Account**:
   - Open the Account > Delete
   - Check Kafka — should see `changeType: DELETE` and `__deleted: true`

#### Troubleshooting

| Issue | Check |
|-------|-------|
| No events in Kafka | Setup > Quick Find > "Apex Jobs" — check for failed `@future` jobs |
| "Unauthorized endpoint" error | Verify Named Credential URL is correct and External Credential Principal Access is granted via Permission Set |
| "Insufficient access" on trigger | Verify your profile has "Modify All Data" and the trigger is active |
| Events in wrong topic | Verify `req.setEndpoint` has no URL path (just `callout:Kafka_Connector`) |
| Missing fields in payload | Check field-level security — the user must have read access to all fields |
| Trigger not firing | Object Manager > your object > Triggers — verify status is "Active" |

#### Limitations

- **`@future` method limit**: 50 per transaction. Bulk operations (e.g., Data Loader importing 50+ records) may hit this limit.
- **Callout timeout**: 120 seconds max per callout.
- **One trigger per object**: You need to create a trigger for each Salesforce object you want to capture.
- **No replay**: If the connector is down when Salesforce sends the event, it's lost. Use the snapshot feature to backfill missed data.
- **Field discovery**: The `WebhookSender` dynamically discovers all fields at runtime, including custom fields added later. No code changes needed when fields change.

---### Important: URL Path and Topic Routing

When using the payload router (`camel.source.payload.router.enabled=true`), the connector automatically determines the Kafka topic from the payload content. The Apex callout endpoint should have **no URL path** — just the base URL:

```apex
// Correct — no path, payload router determines topic
req.setEndpoint('callout:Kafka_Connector');

// Wrong — /webhook path may conflict with CamelDynamicTopicTransform
req.setEndpoint('callout:Kafka_Connector/webhook');
```

If your connector config includes the `CamelDynamicTopicTransform` (which sets topic from the HTTP path), having a path like `/webhook` will override the payload router's topic. With no path, the payload router's topic (`sf_account_events`) is preserved through the transform chain.

### Verifying the Setup

1. **Test with a simple curl:**
   ```bash
   curl -X POST http://your-connector-host:8080 \
     -H "Content-Type: application/json" \
     -d '{
       "data": {
         "payload": {
           "ChangeEventHeader": {
             "entityName": "Account",
             "recordIds": ["001TEST"],
             "changeType": "CREATE"
           },
           "Id": "001TEST",
           "Name": "Test Account"
         },
         "event": {"replayId": 1}
       },
       "channel": "/data/AccountChangeEvent"
     }'
   ```

2. **Check Kafka for the record:**
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 --topic sf_account_events --from-beginning
   ```

3. **Verify the key and value:**
   - Key should contain `{"Id": "001TEST"}`
   - Value should contain `{"Id": "001TEST", "Name": "Test Account", "__deleted": false}`

---

## 4. Fan-out Configuration

Fan-out splits nested arrays into separate Kafka topics, creating one record per array element.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.payload.router.fanout.fields` | String | `""` | Comma-separated `domain.field` pairs |

### Format

```properties
camel.source.payload.router.fanout.fields=ticket.tags,ticket.custom_fields,ticket.comments,ticket.collaborators,ticket.followers,organization.tags
```

Fan-out is **domain-scoped**: `ticket.tags` only fans out tags for ticket events, not organization events. This allows independent control per domain.

### Fan-out Topics

| Config Entry | Generated Topic | Key Schema |
|-------------|-----------------|-----------|
| `ticket.tags` | `{prefix}ticket_tags` | `{detail_id, value}` |
| `ticket.custom_fields` | `{prefix}ticket_custom_fields` | `{detail_id, id}` |
| `ticket.comments` | `{prefix}ticket_comments` | `{id}` |
| `ticket.collaborators` | `{prefix}ticket_collaborators` | `{detail_id, id}` |
| `ticket.followers` | `{prefix}ticket_followers` | `{detail_id, id}` |
| `organization.tags` | `{prefix}organization_tags` | `{detail_id, value}` |

### Fan-out Record Structure

Each fan-out record contains:
- The array element's own fields
- `detail_id` — parent entity ID (matches key field name)
- `_ctx_event_id` — webhook event UUID for correlation

Example fan-out tag record:

```json
{
  "value": "urgent",
  "detail_id": 987654,
  "_ctx_event_id": "event-uuid-123"
}
```

### Fan-out and Deletions

Fan-out topics do NOT generate tombstone records when items are removed. For example, if a tag is removed from a ticket, no delete record is produced for the old tag.

**Recommended sink-side handling:**
- Use delete-and-reinsert mode on the sink connector
- Or treat each ticket event as source of truth and do full replace of child rows

---

## 5. Detail Flattening

Promotes nested `detail` fields to top-level, avoiding nested JSON in the output.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.payload.router.flatten.detail` | Boolean | `false` | Enable detail flattening |
| `camel.source.payload.router.flatten.detail.prefix` | String | `detail_` | Prefix for flattened fields |

### Before Flattening

```json
{
  "type": "zen:event-type:ticket.created",
  "id": "event-uuid",
  "subject": "zen:ticket:987654",
  "detail": {
    "id": 987654,
    "subject": "Cannot login",
    "status": "new"
  }
}
```

### After Flattening (prefix = `detail_`)

```json
{
  "type": "zen:event-type:ticket.created",
  "id": "event-uuid",
  "subject": "zen:ticket:987654",
  "detail_id": 987654,
  "detail_subject": "Cannot login",
  "detail_status": "new",
  "__deleted": false
}
```

The prefix prevents collisions between top-level `id` (event UUID) and `detail.id` (entity ID).

For Salesforce CDC with empty prefix (`""`), fields are promoted directly since Salesforce PascalCase field names (`Name`, `BillingCity`) don't collide with metadata fields.

---

## 6. Event Field Control

Controls whether the `event` field (Zendesk) or `ChangeEventHeader` (Salesforce) is included in output.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.payload.router.include.event` | Boolean | `true` | Include event/change metadata in output |

### Modes

| `flatten.detail` | `include.event` | Output | Use Case |
|---|---|---|---|
| `false` | `true` | Full payload as-is | Append / audit log |
| `false` | `false` | Full payload minus event field | Append, less noise |
| `true` | `true` | Flat detail fields + event nested | Mixed |
| `true` | `false` | Clean flat state record | **Upsert / state table** |

---

## 7. Message Keys

Message keys are set automatically based on the provider's data model. No configuration needed.

Keys are Kafka Connect **Struct** types with named fields, enabling sink connectors to map key fields to database columns.

Key field names always match a field present in the value, so sink connectors can read from either key or value for upsert operations.

When `flatten.detail.prefix` is changed (e.g., to `d_`), key field names update accordingly (`d_id` instead of `detail_id`).

---

## 8. Delete Detection

Every main event record includes a `__deleted` boolean field.

### Zendesk Delete Rules

| Event Name Pattern | `__deleted` | Examples |
|-------------------|------------|---------|
| Ends with `deleted` | `true` | `deleted`, `permanently_deleted`, `channel_deleted`, `group_membership_deleted` |
| Ends with `removed` | `true` | `removed`, `vote_removed`, `work_item_removed` |
| `soft_deleted` | `false` | Recoverable (in trash) |
| `undeleted` | `false` | Restore from trash |
| Everything else | `false` | `created`, `updated`, `comment_added`, etc. |

### Salesforce Delete Rules

| Change Type | `__deleted` |
|------------|------------|
| `DELETE` | `true` |
| `GAP_DELETE` | `true` |
| `UNDELETE`, `GAP_UNDELETE` | `false` |
| `CREATE`, `UPDATE` | `false` |
| PushTopic `deleted` | `true` |
| PushTopic `undeleted` | `false` |
| Platform Events | Always `false` |

---

## 9. Source Dead Letter Queue

Captures errored records that would otherwise be lost or crash the connector.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.dlq.enabled` | Boolean | `false` | Enable source DLQ |
| `camel.source.dlq.topic` | String | `source_dlq` | DLQ topic name |
| `camel.source.dlq.bootstrap.servers` | String | `""` | Bootstrap servers (empty = use worker's) |

### DLQ Record Format

- **Value**: Raw JSON payload (original webhook body)
- **Serializer**: `StringSerializer` (no schema registry dependency)
- **Headers**:
  - `dlq.error.class` — Exception class name
  - `dlq.error.message` — Error description
  - `dlq.intended.topic` — Where the record was supposed to go
  - `dlq.event.type` — Event type
  - `dlq.timestamp` — When the error occurred

### What Goes to DLQ

| Error Type | DLQ? |
|-----------|------|
| Routing error (bad JSON, unknown fields) | Yes |
| Unexpected processing error | Yes |
| Kafka broker down | No (DLQ also needs Kafka) |

### Recommended Companion Config

```properties
# Safety net for transform errors that slip past poll()
errors.tolerance=all
errors.log.enable=true
errors.log.include.messages=true
```

---

## 10. Snapshot

Debezium-style snapshot support for initial data load and on-demand backfill.

### Provider Support

Snapshot requires a provider-specific `ChunkReader` implementation to query the source API. If the provider does not support snapshots, all snapshot configuration is silently ignored and no resources (thread pools, signal consumers) are created.

| Provider | Snapshot Support | ChunkReader |
|----------|-----------------|-------------|
| Salesforce | Yes | SOQL REST API with Id-based pagination |
| Zendesk | Not yet | Planned (Incremental Export API) |

If you configure `snapshot.mode=initial` or `snapshot.signal.topic` for a provider without snapshot support (e.g., Zendesk), the connector logs a warning and continues with CDC only. No thread pools, signal consumers, or coordinators are started.

### Snapshot Modes

| Mode | Automatic Snapshot | CDC After | Signal Snapshots |
|------|-------------------|----------|-----------------|
| `initial` | Yes (first run, blocking) | Yes | Yes |
| `initial_only` | Yes (first run, blocking) | No (stops) | No |
| `no_data` | No | Yes | Yes |

**Important**: Incremental snapshots are NOT a mode. They are always triggered on-demand via the signal topic, regardless of the configured mode. Even in `no_data` mode, a signal can trigger an incremental or blocking snapshot.

### How Snapshot Modes Interact with CDC

| Mode | During Automatic Snapshot | After Automatic Snapshot | Signal-Triggered Snapshot |
|------|--------------------------|-------------------------|--------------------------|
| `initial` | CDC paused (blocking) | CDC resumes | Incremental runs alongside CDC with dedup |
| `initial_only` | CDC paused (blocking) | Connector stops | N/A (connector stopped) |
| `no_data` | N/A (no automatic snapshot) | CDC runs immediately | Incremental runs alongside CDC with dedup |

For `BLOCKING` signal-triggered snapshots, CDC is paused during execution. For `INCREMENTAL` signal-triggered snapshots, CDC continues with windowed deduplication.

### Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.snapshot.mode` | String | `no_data` | Snapshot mode: `initial`, `initial_only`, `no_data` |
| `camel.source.snapshot.objects` | String | `""` | Objects for initial mode (CSV) |
| `camel.source.snapshot.signal.topic` | String | `""` | Kafka topic for signal commands |
| `camel.source.snapshot.signal.poll.interval.ms` | Long | `5000` | Signal poll interval (ms) |
| `camel.source.snapshot.max.threads` | Integer | `4` | Max parallel threads |
| `camel.source.snapshot.chunk.size` | Integer | `1000` | Records per chunk |
| `camel.source.snapshot.chunk.delay.ms` | Long | `500` | Delay between chunks (ms) |
| `camel.source.snapshot.parallel.segments.enabled` | Boolean | `true` | Split large objects into parallel segments |
| `camel.source.snapshot.parallel.segments.min.rows` | Long | `10000` | Min rows to trigger segment splitting |

### Salesforce Snapshot API Config

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.snapshot.salesforce.instance.url` | String | `""` | Salesforce instance URL |
| `camel.source.snapshot.salesforce.auth.client.id` | String | `""` | OAuth2 client ID |
| `camel.source.snapshot.salesforce.auth.client.secret` | Password | `""` | OAuth2 client secret |
| `camel.source.snapshot.salesforce.auth.username` | String | `""` | Username |
| `camel.source.snapshot.salesforce.auth.password` | Password | `""` | Password + security token |

#### Why Both Client Credentials and User Credentials?

Salesforce OAuth2 username-password flow requires two layers of authentication:

- **Client ID + Client Secret** identify the **application** (the connector). Salesforce needs to know which app is making API calls.
- **Username + Password** identify the **user** whose data access permissions are used to query records. Salesforce enforces object-level and field-level security based on this user's profile.

Both are required because Salesforce won't allow an anonymous application to query data — it must authenticate both the app and the user making the request.

#### How to Obtain Credentials

**Step 1: Create a Connected App (for Client ID and Secret)**

1. Log in to Salesforce as an admin
2. Navigate to **Setup** > **App Manager** > **New Connected App**
3. Fill in basic info (name, contact email)
4. Under **API (Enable OAuth Settings)**:
   - Check "Enable OAuth Settings"
   - Callback URL: `https://login.salesforce.com/services/oauth2/callback`
   - Selected OAuth Scopes: add `api` and `full`
5. Save the Connected App
6. After saving, navigate to the app's detail page:
   - **Consumer Key** = use as `auth.client.id`
   - **Consumer Secret** = use as `auth.client.secret`

Note: It may take 2-10 minutes for a new Connected App to become active.

**Step 2: Get Username**

Use the Salesforce user's login email address (e.g., `admin@mycompany.com`).

It is recommended to create a dedicated **integration user** with:
- API Enabled permission
- Read access to the objects being snapshotted
- No MFA requirement (or use an app-specific password)

**Step 3: Get Password + Security Token**

The password field must contain the user's password with the **security token appended** directly (no spaces or separators).

To obtain or reset the security token:
1. Log in to Salesforce as the integration user
2. Navigate to **Settings** > **My Personal Information** > **Reset My Security Token**
3. Click "Reset Security Token" — a new token is emailed to the user

If the password is `MyPass123` and the security token is `AbCdEfGh`, set:
```
camel.source.snapshot.salesforce.auth.password=MyPass123AbCdEfGh
```

**Note on IP Whitelisting**: If the connector's IP address is added to the Salesforce org's trusted IP ranges (Setup > Network Access), the security token is not required — just the password alone is sufficient.

**Step 4: Instance URL**

The instance URL is your Salesforce org's domain:
- Standard: `https://mycompany.my.salesforce.com`
- Sandbox: `https://mycompany--sandbox.sandbox.my.salesforce.com`
- Legacy: `https://na1.salesforce.com` (check your browser URL when logged in)

### Signal Topic Commands

Send JSON messages to the configured signal topic to control snapshots.

#### Execute Snapshot

```json
{
  "id": "unique-signal-id",
  "type": "execute-snapshot",
  "data": {
    "objects": ["Account", "Contact", "Lead"],
    "snapshot_type": "INCREMENTAL",
    "chunk_size": 2000,
    "additional_condition": "LastModifiedDate > 2024-01-01T00:00:00Z"
  }
}
```

| Field | Required | Default | Description |
|-------|---------|---------|-------------|
| `objects` | Yes | — | List of objects to snapshot |
| `snapshot_type` | No | `INCREMENTAL` | `INCREMENTAL` or `BLOCKING` |
| `chunk_size` | No | Config default | Override chunk size |
| `additional_condition` | No | — | WHERE clause filter |

#### Stop Snapshot

```json
// Stop specific objects
{"id": "sig-001", "type": "stop-snapshot", "data": {"objects": ["Account"]}}

// Stop ALL active snapshots
{"id": "sig-002", "type": "stop-snapshot", "data": {}}
```

Stops snapshot and discards progress. To restart, send a new `execute-snapshot`.

#### Pause Snapshot

```json
// Pause specific objects
{"id": "sig-003", "type": "pause-snapshot", "data": {"objects": ["Account"]}}

// Pause ALL
{"id": "sig-004", "type": "pause-snapshot", "data": {}}
```

Preserves position. Resume with `resume-snapshot`.

#### Resume Snapshot

```json
// Resume specific objects
{"id": "sig-005", "type": "resume-snapshot", "data": {"objects": ["Account"]}}

// Resume ALL paused
{"id": "sig-006", "type": "resume-snapshot", "data": {}}
```

### Parallel Execution

The snapshot engine supports parallel execution at two levels:

**Multi-object parallelism**: When snapshotting multiple objects (e.g., Account + Contact + Lead), each object runs in its own thread.

**Single-object segment parallelism**: When snapshotting one large object, the key range is split into N segments, each running independently. This reduces snapshot time proportionally.

Example: Account table with 1M rows, `max.threads=4`:
- Segment 0: Id range [start, boundary1)
- Segment 1: Id range [boundary1, boundary2)
- Segment 2: Id range [boundary2, boundary3)
- Segment 3: Id range [boundary3, end]

Each segment has its own offset tracking and is independently restartable.

### Deduplication

When incremental snapshot runs alongside CDC, the same record may appear in both streams. The engine uses a **windowing** mechanism:

1. **Window opens** — Snapshot chunk is read, records stored in memory window
2. **CDC event arrives** for a record in the window — CDC version wins, record removed from window
3. **Window closes** — Remaining window records (not seen in CDC) are emitted

This ensures each record is emitted exactly once, with CDC (latest version) taking priority.

### Offset Tracking and Restart Recovery

Snapshot progress is stored in Kafka Connect's offset storage per segment:

```
Partition: {connector: "my-connector", snapshot_object: "Account", segment: "0"}
Offset: {status: "IN_PROGRESS", last_key: "001D000000KnaXjIAJ", max_key: "001D000000ZzzIAJ", chunk_number: 15}
```

On connector restart:
- Completed segments are skipped
- In-progress segments resume from `last_key`
- At most one chunk of data is re-read (at-least-once guarantee)

### Connector Startup Flow

```
start()
  ├── Initialize Camel context + Netty HTTP server
  ├── Initialize PayloadRouter (routing strategy)
  ├── Initialize DLQ producer (if enabled)
  └── Initialize Snapshot Engine
        ├── Is snapshot needed? (mode != no_data OR signal topic configured?)
        │     NO → skip, no resources created
        │     YES ↓
        ├── Create ChunkReader via factory
        │     Provider has implementation? (e.g. Salesforce)
        │       NO → log warning, skip, no resources created
        │       YES ↓
        ├── Create SnapshotCoordinator (thread pool)
        ├── Create SnapshotSignalConsumer (if signal topic configured)
        ├── Create SnapshotEngine
        └── Start engine
              └── If mode=initial and first run → start blocking snapshot
```

### Poll Loop Flow

```
poll()
  ├── 1. Poll snapshot engine (if active)
  │     ├── Check signal topic for commands
  │     └── Drain snapshot records from coordinator
  │
  ├── 2. Should CDC stream?
  │     ├── Blocking snapshot in progress? → return snapshot records only
  │     ├── initial_only mode and snapshot done? → return null (stop)
  │     └── Otherwise → proceed to CDC
  │
  └── 3. CDC path
        ├── Receive webhook exchanges
        ├── Route through PayloadRouter
        ├── Dedup against open snapshot windows (if any)
        └── DLQ on errors, merge with snapshot records
```

### Resource Management

The snapshot engine is designed to use zero resources when not needed:

| Scenario | Thread Pool | Signal Consumer | Memory |
|----------|------------|----------------|--------|
| `mode=no_data`, no signal topic | Not created | Not created | None |
| `mode=no_data`, signal topic, provider without ChunkReader (e.g. Zendesk) | Not created | Not created | None |
| `mode=no_data`, signal topic, Salesforce | Created (idle until signal) | Created | Minimal |
| `mode=initial`, Salesforce | Created + active | Created | Window + output queue (bounded at 10,000 records) |
| Snapshot completes | Pool idle | Still polling signals | Minimal |

The snapshot output queue is bounded (10,000 records max). When full, snapshot threads block until poll() drains records. This prevents unbounded memory growth during large snapshots.

### Provider-Specific Behavior

#### Salesforce

- Uses SOQL REST API (`SELECT FIELDS(ALL) FROM {Object} WHERE Id > :lastKey ORDER BY Id LIMIT :chunkSize`)
- OAuth2 username-password flow for authentication
- Token auto-refreshed every 90 minutes
- Segment boundaries calculated via count + offset queries
- `FIELDS(ALL)` returns all accessible fields automatically

#### Zendesk (Planned)

- Will use Incremental Export API (`GET /api/v2/incremental/{resource}.json?start_time=`)
- Rate limited at ~10 requests per minute
- Cursor-based pagination via `end_time` field
- Not yet implemented — snapshot config is ignored for Zendesk

---

## 11. Full Configuration Reference

### Payload Routing

| Property | Type | Default |
|----------|------|---------|
| `camel.source.payload.router.enabled` | Boolean | `false` |
| `camel.source.payload.router.type` | String | — |
| `camel.source.payload.router.topic.prefix` | String | `""` |
| `camel.source.payload.router.unknown.type.behavior` | String | `DEFAULT_TOPIC` |
| `camel.source.payload.router.unknown.type.default.topic` | String | `unknown` |
| `camel.source.payload.router.fanout.fields` | String | `""` |
| `camel.source.payload.router.flatten.detail` | Boolean | `false` |
| `camel.source.payload.router.flatten.detail.prefix` | String | `detail_` |
| `camel.source.payload.router.include.event` | Boolean | `true` |

### Source DLQ

| Property | Type | Default |
|----------|------|---------|
| `camel.source.dlq.enabled` | Boolean | `false` |
| `camel.source.dlq.topic` | String | `source_dlq` |
| `camel.source.dlq.bootstrap.servers` | String | `""` |

### Snapshot

| Property | Type | Default |
|----------|------|---------|
| `camel.source.snapshot.mode` | String | `no_data` |
| `camel.source.snapshot.objects` | String | `""` |
| `camel.source.snapshot.signal.topic` | String | `""` |
| `camel.source.snapshot.signal.poll.interval.ms` | Long | `5000` |
| `camel.source.snapshot.max.threads` | Integer | `4` |
| `camel.source.snapshot.chunk.size` | Integer | `1000` |
| `camel.source.snapshot.chunk.delay.ms` | Long | `500` |
| `camel.source.snapshot.parallel.segments.enabled` | Boolean | `true` |
| `camel.source.snapshot.parallel.segments.min.rows` | Long | `10000` |
| `camel.source.snapshot.salesforce.instance.url` | String | `""` |
| `camel.source.snapshot.salesforce.auth.client.id` | String | `""` |
| `camel.source.snapshot.salesforce.auth.client.secret` | Password | `""` |
| `camel.source.snapshot.salesforce.auth.username` | String | `""` |
| `camel.source.snapshot.salesforce.auth.password` | Password | `""` |

---

## 12. Example Configurations

### Zendesk — Upsert Mode (State Table)

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=zendesk
camel.source.payload.router.topic.prefix=zendesk_
camel.source.payload.router.unknown.type.behavior=SKIP
camel.source.payload.router.fanout.fields=ticket.tags,ticket.custom_fields,organization.tags
camel.source.payload.router.flatten.detail=true
camel.source.payload.router.flatten.detail.prefix=detail_
camel.source.payload.router.include.event=false

camel.source.dlq.enabled=true
camel.source.dlq.topic=zendesk_dlq

errors.tolerance=all
errors.log.enable=true
```

### Zendesk — Append Mode (Audit Log)

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=zendesk
camel.source.payload.router.topic.prefix=zendesk_
camel.source.payload.router.unknown.type.behavior=DEFAULT_TOPIC
camel.source.payload.router.unknown.type.default.topic=unknown
camel.source.payload.router.fanout.fields=
camel.source.payload.router.flatten.detail=false
camel.source.payload.router.include.event=true

camel.source.dlq.enabled=true
camel.source.dlq.topic=zendesk_dlq
```

### Salesforce — CDC with Initial Snapshot

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=salesforce
camel.source.payload.router.topic.prefix=sf_
camel.source.payload.router.flatten.detail=true
camel.source.payload.router.flatten.detail.prefix=
camel.source.payload.router.include.event=false

camel.source.snapshot.mode=initial
camel.source.snapshot.objects=Account,Contact,Lead,Opportunity
camel.source.snapshot.max.threads=4
camel.source.snapshot.chunk.size=1000
camel.source.snapshot.signal.topic=sf_snapshot_signals

camel.source.snapshot.salesforce.instance.url=https://myorg.salesforce.com
camel.source.snapshot.salesforce.auth.client.id=your_client_id
camel.source.snapshot.salesforce.auth.client.secret=your_client_secret
camel.source.snapshot.salesforce.auth.username=your_username
camel.source.snapshot.salesforce.auth.password=your_password_with_token

camel.source.dlq.enabled=true
camel.source.dlq.topic=sf_dlq

errors.tolerance=all
errors.log.enable=true
```

### Salesforce — CDC Only with On-Demand Snapshots

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=salesforce
camel.source.payload.router.topic.prefix=sf_
camel.source.payload.router.flatten.detail=true
camel.source.payload.router.flatten.detail.prefix=
camel.source.payload.router.include.event=false

camel.source.snapshot.mode=no_data
camel.source.snapshot.signal.topic=sf_snapshot_signals
camel.source.snapshot.max.threads=4
camel.source.snapshot.parallel.segments.enabled=true

camel.source.snapshot.salesforce.instance.url=https://myorg.salesforce.com
camel.source.snapshot.salesforce.auth.client.id=your_client_id
camel.source.snapshot.salesforce.auth.client.secret=your_client_secret
camel.source.snapshot.salesforce.auth.username=your_username
camel.source.snapshot.salesforce.auth.password=your_password_with_token
```

Then trigger snapshots on demand:

```json
{"id": "backfill-001", "type": "execute-snapshot", "data": {"objects": ["Account"], "snapshot_type": "INCREMENTAL", "chunk_size": 2000}}
```
