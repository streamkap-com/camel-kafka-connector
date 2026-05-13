# Salesforce Configuration

Supported when `camel.source.payload.router.type=salesforce`.

## Supported Event Formats

### Change Data Capture (CDC)

Detected by presence of `data.payload.ChangeEventHeader`.

| changeType | Topic | `__changeType` | `__op` (header) | `__deleted` |
|-----------|-------|----------------|-----------------|------------|
| `CREATE` | `{prefix}{entity}` | `CREATE` | `c` | `false` |
| `UPDATE` | `{prefix}{entity}` | `UPDATE` | `u` | `false` |
| `DELETE` | `{prefix}{entity}` | `DELETE` | `d` | `true` |
| `UNDELETE` | `{prefix}{entity}` | `UNDELETE` | `u` | `false` |
| `GAP_CREATE` | `{prefix}gap` | `GAP_CREATE` | `c` | `false` |
| `GAP_DELETE` | `{prefix}gap` | `GAP_DELETE` | `d` | `true` |
| `GAP_OVERFLOW` | `{prefix}gap` | `GAP_OVERFLOW` | `u` | `false` |

### PushTopic Events

Detected by presence of `data.sobject`.

| event.type | Topic | `__changeType` | `__op` (header) |
|-----------|-------|----------------|-----------------|
| `created` | `{prefix}{topicname}` | `CREATED` | `c` |
| `updated` | `{prefix}{topicname}` | `UPDATED` | `u` |
| `deleted` | `{prefix}{topicname}` | `DELETED` | `d` |
| `undeleted` | `{prefix}{topicname}` | `UNDELETED` | `u` |

### Platform Events

Detected by presence of `data.payload` without `ChangeEventHeader`.

| | Topic | `__changeType` |
|---|-------|----------------|
| All | `{prefix}{eventname}` | `PUBLISHED` |

Platform events always have `__deleted = false` and `__op = "c"` (header).

## Message Keys

All Salesforce events use `{Id: <record_id>}` as key (or `{<prefix>Id: <record_id>}` when flattened with a prefix).

## Native CDC Subscription

The connector can subscribe directly to Salesforce's Streaming API (CometD) without webhooks:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.cdc.enabled` | Boolean | `false` | Enable native CDC subscription |
| `camel.source.cdc.channels` | String | `""` | CDC channels (e.g., `/data/ChangeEvents` or `/data/AccountChangeEvent,/data/ContactChangeEvent`) |

## Snapshot

Supports Debezium-style initial and incremental snapshots via the Salesforce Bulk API:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.snapshot.mode` | String | `no_data` | `initial`, `initial_only`, or `no_data` |
| `camel.source.snapshot.objects` | String | `""` | Objects to snapshot (e.g., `Account,Contact,Lead`) |
| `camel.source.snapshot.signal.topic` | String | `""` | Kafka topic for on-demand snapshot signals |
| `camel.source.snapshot.salesforce.instance.url` | String | `""` | Salesforce instance URL |
| `camel.source.snapshot.salesforce.auth.client.id` | String | `""` | OAuth2 client ID |
| `camel.source.snapshot.salesforce.auth.client.secret` | Password | `""` | OAuth2 client secret |
| `camel.source.snapshot.salesforce.auth.username` | String | `""` | Salesforce username |
| `camel.source.snapshot.salesforce.auth.password` | Password | `""` | Salesforce password (with security token) |

Snapshot records include `__changeType: "SNAPSHOT"`, `__deleted: false`, and `__op: "r"` as a Kafka header.

Signal format supports both Debezium-compatible (`data-collections`) and our format (`objects`):
```json
{
  "id": "backfill-accounts",
  "type": "execute-snapshot",
  "data": {
    "data-collections": ["Account", "Contact"],
    "type": "INCREMENTAL"
  }
}
```

## Setting Up Salesforce Webhook Triggers

### Prerequisites

- A Salesforce org (Developer, Professional, Enterprise, or Unlimited edition)
- Admin access (System Administrator profile)
- Your connector's public URL (e.g., `https://your-connector.example.com`)

### Step 1: Create an External Client App

Provides OAuth credentials for Apex callout and snapshot API.

1. **Setup** > Quick Find > **"App Manager"**
2. Click **New External Client App**
3. Fill in: Name: `Kafka Connector`, Contact Email, Distribution State: `Local`
4. Under **Enable OAuth**:
   - Check **Enable OAuth Settings**
   - Callback URL: `https://login.salesforce.com/services/oauth2/callback`
   - Scopes: `Manage user data via APIs (api)`, `Full access (full)`, `Perform requests at any time (refresh_token, offline_access)`
5. Under **Security**: uncheck `Require Proof Key for Code Exchange (PKCE)`
6. Save, note the **Consumer Key** and **Consumer Secret**

### Step 2: Create Named Credential

**External Credential:**

1. **Setup** > **Named Credentials** > **External Credentials** tab > **New**
2. Label: `Kafka Connector Webhook`, Protocol: `Custom`
3. Save > Add Principal: `Default`, Identity Type: `Named Principal`
4. Add Authentication Parameter: Name: `x-api-key`, Value: your API key

**Named Credential:**

1. **Named Credentials** tab > **New**
2. Label: `Kafka Connector`, URL: `https://your-connector.example.com`, External Credential: `Kafka Connector Webhook`

**Permission Set:**

1. Create permission set `Kafka Webhook Access`
2. Add `Kafka_Connector_Webhook - Default` to External Credential Principal Access
3. Assign to your user, log out and back in

### Step 3: Create the Webhook Sender Apex Class

**Setup** > **Apex Classes** > **New**:

```apex
public class WebhookSender {

    @future(callout=true)
    public static void sendAsync(String recordId, String changeType, String objectName) {
        Map<String, Schema.SObjectField> fieldMap = Schema.getGlobalDescribe()
            .get(objectName).getDescribe().fields.getMap();

        List<String> fieldNames = new List<String>();
        for (String fieldName : fieldMap.keySet()) {
            Schema.DescribeFieldResult fieldDesc = fieldMap.get(fieldName).getDescribe();
            if (fieldDesc.isAccessible()) {
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
            recordMap.put(fieldName, record.get(fieldName));
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

**Do not modify the payload structure.** The connector relies on these exact field names:

| Field Path | Purpose |
|-----------|---------|
| `data.payload.ChangeEventHeader` | Identifies event as Salesforce CDC |
| `data.payload.ChangeEventHeader.entityName` | Determines Kafka topic |
| `data.payload.ChangeEventHeader.changeType` | Sets `__deleted` and `__changeType` |
| `data.payload.Id` | Used as message key |
| `data.event.replayId` | Included in output for tracking |

### Step 4: Create Apex Triggers per Object

One trigger per object. Example for Account:

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

To add more objects (Contact, Lead, Opportunity, etc.), create a trigger with the same pattern, changing the object name.

### Step 5: Configure the Connector

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=salesforce
camel.source.payload.router.topic.prefix=sf_
camel.source.payload.router.flatten.detail=true
camel.source.payload.router.flatten.detail.prefix=detail_
camel.source.payload.router.include.event=false
camel.source.dlq.enabled=true
camel.source.dlq.topic=sf_dlq
```

### URL Path and Topic Routing

The Apex callout endpoint must have **no URL path**:

```apex
// Correct
req.setEndpoint('callout:Kafka_Connector');

// Wrong - path conflicts with topic routing
req.setEndpoint('callout:Kafka_Connector/webhook');
```

### Testing with curl

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

### Troubleshooting

| Issue | Check |
|-------|-------|
| No events in Kafka | Setup > "Apex Jobs" -- check for failed `@future` jobs |
| "Unauthorized endpoint" | Verify Named Credential URL and Permission Set |
| Events in wrong topic | Verify `req.setEndpoint` has no URL path |
| Missing fields | Check field-level security for the user |
| Trigger not firing | Object Manager > Triggers -- verify "Active" |

### Limitations

- **`@future` limit**: 50 per transaction (bulk operations may hit this)
- **Callout timeout**: 120 seconds max
- **One trigger per object**
- **No replay**: Use snapshot to backfill missed data
- **Dynamic field discovery**: Custom fields added later are automatically included
