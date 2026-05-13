# Camel Netty HTTP Kafka Source Connector — Configuration Guide

## Overview

This connector receives webhook HTTP POST events from external applications (Zendesk, Salesforce, Shopify, etc.) and routes them to Kafka topics. It provides:

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
4. [Shopify Configuration](#4-shopify-configuration)
5. [Fan-out Configuration](#5-fan-out-configuration)
6. [Detail Flattening](#6-detail-flattening)
7. [Event Field Control](#7-event-field-control)
8. [Message Keys](#8-message-keys)
9. [Delete Detection](#9-delete-detection)
10. [Source Dead Letter Queue](#10-source-dead-letter-queue)
11. [Snapshot](#11-snapshot)
12. [Full Configuration Reference](#12-full-configuration-reference)
13. [Example Configurations](#13-example-configurations)

---

## 1. Payload Routing

The connector inspects incoming JSON payloads to determine the target Kafka topic and message key.

### Base Config

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.payload.router.enabled` | Boolean | `false` | Enable payload-based routing |
| `camel.source.payload.router.type` | String | — | Provider type: `zendesk`, `salesforce`, `shopify` |
| `camel.source.payload.router.topic.prefix` | String | `""` | Prefix for all generated topic names |
| `camel.source.payload.router.unknown.type.behavior` | String | `DEFAULT_TOPIC` | How to handle unrecognized events: `DEFAULT_TOPIC`, `SKIP`, `FAIL` |
| `camel.source.payload.router.unknown.type.default.topic` | String | `unknown` | Topic for unknown events (when behavior = DEFAULT_TOPIC) |

---

## 2. Zendesk Configuration

Supported when `camel.source.payload.router.type=zendesk`. Routes events by domain (tickets, users, organizations, etc.) based on the `type` field in the JSON body.

| Zendesk Domain | Generated Topic | Key |
|---|---|---|
| Tickets | `{prefix}ticket_events` | `{detail_id}` |
| Users | `{prefix}user_events` | `{detail_id}` |
| Organizations | `{prefix}organization_events` | `{detail_id}` |
| Articles | `{prefix}article_events` | `{detail_id}` |
| Agents | `{prefix}agent_events` | `{detail_agent_id}` |

Fan-out: `ticket.tags`, `ticket.custom_fields`, `ticket.comments`, `ticket.collaborators`, `ticket.followers`, `organization.tags`

**Full documentation**: [docs/ZENDESK.md](docs/ZENDESK.md)

---

## 3. Salesforce Configuration

Supported when `camel.source.payload.router.type=salesforce`. Supports CDC, PushTopic, and Platform Event formats. Includes native CDC subscription and Debezium-style snapshots.

| Event Format | Detection | Topic Pattern | Key |
|---|---|---|---|
| CDC | `data.payload.ChangeEventHeader` | `{prefix}{entity}` | `{Id}` |
| PushTopic | `data.sobject` | `{prefix}{topicname}` | `{Id}` |
| Platform Event | `data.payload` (no header) | `{prefix}{eventname}` | `{Id}` |

Includes webhook setup guide (Apex triggers, Named Credentials), native CDC subscription, and snapshot configuration.

**Full documentation**: [docs/SALESFORCE.md](docs/SALESFORCE.md)

---

## 4. Shopify Configuration

Supported when `camel.source.payload.router.type=shopify`. Routes events based on the `X-Shopify-Topic` HTTP header (e.g., `orders/create`). Includes HMAC signature verification.

| Resource | Generated Topic | Key | Example Topic Header |
|---|---|---|---|
| Orders | `{prefix}orders` | `{id}` | `orders/create`, `orders/cancelled` |
| Products | `{prefix}products` | `{id}` | `products/update` |
| Customers | `{prefix}customers` | `{id}` | `customers/delete` |
| Draft Orders | `{prefix}draft_orders` | `{id}` | `draft_orders/create` |
| Fulfillments | `{prefix}fulfillments` | `{id}` | `fulfillments/create` |
| Collections | `{prefix}collections` | `{id}` | `collections/create` |
| Inventory | `{prefix}inventory_items` | `{id}` | `inventory_items/update` |

Fan-out: `orders.line_items`, `orders.shipping_lines`, `orders.tax_lines`, `products.variants`, `products.images`, `customers.addresses`

HMAC config: `camel.source.payload.router.shopify.hmac.secret` (Password)

**Full documentation**: [docs/SHOPIFY.md](docs/SHOPIFY.md)

---

## 5. Fan-out Configuration

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

## 6. Detail Flattening

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

## 7. Event Field Control

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

## 8. Message Keys

Message keys are set automatically based on the provider's data model. No configuration needed.

Keys are Kafka Connect **Struct** types with named fields, enabling sink connectors to map key fields to database columns.

Key field names always match a field present in the value, so sink connectors can read from either key or value for upsert operations.

When `flatten.detail.prefix` is changed (e.g., to `d_`), key field names update accordingly (`d_id` instead of `detail_id`).

---

## 9. Delete Detection and Operation Type

Every record includes:
- `__deleted` (boolean, in value) — `true` for deletes, `false` for everything else
- `__changeType` (string, in value) — full action name (CREATE, UPDATE, DELETE, SNAPSHOT, etc.)
- `__op` (string, in Kafka header) — Debezium-compatible operation: `c` (create), `u` (update), `d` (delete), `r` (read/snapshot)

### Operation Type Mapping

| `__op` | Meaning | When |
|--------|---------|------|
| `c` | Create | New record created |
| `u` | Update | Record updated (includes state changes like cancelled, fulfilled) |
| `d` | Delete | Record permanently deleted |
| `r` | Read | Snapshot backfill record |

### Zendesk Delete Rules

| Event Name Pattern | `__deleted` | `__op` | Examples |
|-------------------|------------|--------|---------|
| Ends with `deleted` | `true` | `d` | `deleted`, `permanently_deleted`, `channel_deleted` |
| Ends with `removed` | `true` | `d` | `removed`, `vote_removed`, `work_item_removed` |
| `soft_deleted` | `false` | `u` | Recoverable (in trash) |
| `undeleted` | `false` | `u` | Restore from trash |
| `created` | `false` | `c` | New record |
| Everything else | `false` | `u` | `updated`, `comment_added`, etc. |

### Salesforce Delete Rules

| Change Type | `__deleted` | `__op` |
|------------|------------|--------|
| `CREATE` | `false` | `c` |
| `UPDATE` | `false` | `u` |
| `DELETE` | `true` | `d` |
| `UNDELETE` | `false` | `u` |
| `GAP_DELETE` | `true` | `d` |
| `GAP_CREATE` | `false` | `c` |
| PushTopic `deleted` | `true` | `d` |
| PushTopic `undeleted` | `false` | `u` |
| Platform Events | Always `false` | `c` |
| Snapshot | `false` | `r` |

### Shopify Delete Rules

| Action | `__deleted` | `__op` |
|--------|------------|--------|
| `create` | `false` | `c` |
| `update` / `updated` | `false` | `u` |
| `delete` | `true` | `d` |
| `cancelled`, `fulfilled`, `paid`, etc. | `false` | `u` |
| Snapshot | `false` | `r` |

---

## 10. Source Dead Letter Queue

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

## 11. Snapshot

Debezium-style snapshot support for initial data load and on-demand backfill.

### Provider Support

Snapshot requires a provider-specific `ChunkReader` implementation to query the source API. If the provider does not support snapshots, all snapshot configuration is silently ignored and no resources (thread pools, signal consumers) are created.

| Provider | Snapshot Support | ChunkReader | Auth |
|----------|-----------------|-------------|------|
| Salesforce | Yes | SOQL REST API with Id-based pagination | OAuth2 (client credentials or password) |
| Shopify | Yes | GraphQL Admin API with cursor pagination (250/page) | Static token or client credentials (24h refresh) |
| Zendesk | Not yet | Planned (Incremental Export API) | — |

If you configure `snapshot.mode=initial` or `snapshot.signal.topic` for a provider without snapshot support (e.g., Zendesk), the connector logs a warning and continues with webhooks only. No thread pools, signal consumers, or coordinators are started.

Provider-specific snapshot configuration: [docs/SALESFORCE.md](docs/SALESFORCE.md#snapshot), [docs/SHOPIFY.md](docs/SHOPIFY.md#snapshot-backfill).

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

## 12. Full Configuration Reference

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

### Shopify

| Property | Type | Default |
|----------|------|---------|
| `camel.source.payload.router.shopify.hmac.secret` | Password | `""` |

### Native CDC

| Property | Type | Default |
|----------|------|---------|
| `camel.source.cdc.enabled` | Boolean | `false` |
| `camel.source.cdc.channels` | String | `""` |

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
| `camel.source.snapshot.shopify.store.url` | String | `""` |
| `camel.source.snapshot.shopify.access.token` | Password | `""` |
| `camel.source.snapshot.shopify.client.id` | String | `""` |
| `camel.source.snapshot.shopify.client.secret` | Password | `""` |
| `camel.source.snapshot.shopify.api.version` | String | `2024-10` |

---

## 13. Example Configurations

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
