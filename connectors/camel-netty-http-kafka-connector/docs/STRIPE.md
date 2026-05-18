# Stripe Configuration

Supported when `camel.source.payload.router.type=stripe`.

Stripe sends the event type in the JSON body (`type` field), not in HTTP headers. The payload is an event envelope containing `data.object` (the actual resource). The strategy extracts `data.object` as the Kafka record value.

## How It Works

Stripe webhooks are HTTP POST requests with a JSON event envelope:

```json
{
  "id": "evt_1MqqbKLt4dXkbwBz7USMV0c7",
  "object": "event",
  "api_version": "2023-10-16",
  "created": 1680064028,
  "livemode": true,
  "type": "customer.created",
  "data": {
    "object": {
      "id": "cus_abc123",
      "email": "customer@example.com",
      "name": "John Doe"
    }
  }
}
```

The connector extracts `data.object` as the Kafka record value, adds synthetic fields (`__changeType`, `__deleted`), and optionally adds event metadata.

## Resource Topics

Event types use dot notation: `resource.action` or `resource.sub_resource.action`. The first segment determines the Kafka topic:

| Event Type | Resource | Topic | Key |
|---|---|---|---|
| `customer.created` | `customer` | `{prefix}customer` | `{id: cus_xxx}` |
| `customer.updated` | `customer` | `{prefix}customer` | `{id: cus_xxx}` |
| `customer.deleted` | `customer` | `{prefix}customer` | `{id: cus_xxx}` |
| `customer.subscription.created` | `customer` | `{prefix}customer` | `{id: sub_xxx}` |
| `customer.subscription.updated` | `customer` | `{prefix}customer` | `{id: sub_xxx}` |
| `payment_intent.succeeded` | `payment_intent` | `{prefix}payment_intent` | `{id: pi_xxx}` |
| `payment_intent.payment_failed` | `payment_intent` | `{prefix}payment_intent` | `{id: pi_xxx}` |
| `charge.succeeded` | `charge` | `{prefix}charge` | `{id: ch_xxx}` |
| `charge.refunded` | `charge` | `{prefix}charge` | `{id: ch_xxx}` |
| `charge.dispute.created` | `charge` | `{prefix}charge` | `{id: dp_xxx}` |
| `invoice.paid` | `invoice` | `{prefix}invoice` | `{id: in_xxx}` |
| `invoice.finalized` | `invoice` | `{prefix}invoice` | `{id: in_xxx}` |
| `product.created` | `product` | `{prefix}product` | `{id: prod_xxx}` |
| `price.created` | `price` | `{prefix}price` | `{id: price_xxx}` |
| `payout.paid` | `payout` | `{prefix}payout` | `{id: po_xxx}` |
| `refund.created` | `refund` | `{prefix}refund` | `{id: re_xxx}` |

## Change Type Mapping

Each record gets `__changeType` and `__deleted` in the value, and `__op` as a Kafka header:

| Action (last segment) | `__changeType` | `__op` (header) | `__deleted` |
|---|---|---|---|
| `created` | `CREATE` | `c` | `false` |
| `updated` | `UPDATE` | `u` | `false` |
| `deleted` | `DELETE` | `d` | `true` |
| `succeeded` | `SUCCEEDED` | `u` | `false` |
| `failed` / `payment_failed` | `FAILED` | `u` | `false` |
| `canceled` | `CANCELED` | `u` | `false` |
| `refunded` | `REFUNDED` | `u` | `false` |
| `captured` | `CAPTURED` | `u` | `false` |
| `expired` | `EXPIRED` | `u` | `false` |
| `paid` | `PAID` | `u` | `false` |
| `finalized` | `FINALIZED` | `u` | `false` |
| `voided` | `VOIDED` | `u` | `false` |
| `completed` | `COMPLETED` | `u` | `false` |
| Snapshot | `SNAPSHOT` | `r` | `false` |
| Any other | `ACTION.toUpperCase()` | `u` | `false` |

## Event Metadata

When `camel.source.payload.router.include.event=true` (default), these fields are added to the payload:

| Field | Source |
|---|---|
| `_event_id` | `id` (event ID, e.g., `evt_xxx`) |
| `_event_type` | `type` (full event type string) |
| `_event_created` | `created` (Unix timestamp) |
| `_api_version` | `api_version` |
| `_livemode` | `livemode` (boolean) |
| `_previous_attributes` | `data.previous_attributes` (for `*.updated` events only) |

Set `include.event=false` to omit metadata.

## Fan-Out

Stripe resources contain nested list objects (`{object: "list", data: [...]}`):

| Config | Array Field | Fan-out Topic | Key |
|---|---|---|---|
| `invoice.lines` | `lines.data[]` | `{prefix}invoice_lines` | `{id: invoice_id, item_id: line_id}` |
| `charge.refunds` | `refunds.data[]` | `{prefix}charge_refunds` | `{id: charge_id, item_id: refund_id}` |
| `subscription.items` | `items.data[]` | `{prefix}subscription_items` | `{id: sub_id, item_id: si_id}` |

## Signature Verification

Stripe signs webhooks with HMAC-SHA256. The algorithm is different from Shopify:

1. Parse `Stripe-Signature` header: `t=timestamp,v1=hex_signature`
2. Compute HMAC-SHA256 of `{timestamp}.{raw_body}` using the signing secret
3. Compare hex-encoded result to `v1` value
4. Check timestamp is within tolerance (default 5 minutes)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.payload.router.stripe.signing.secret` | Password | `""` | Webhook signing secret (`whsec_xxx`). Leave empty to disable. |

During secret rotation, Stripe sends multiple `v1` signatures. The connector checks all of them.

## Allowed Objects Filter

Use `topic.include.list.user.defined` to restrict which Stripe resource types are processed:

```properties
topic.include.list.user.defined=customer,payment_intent,invoice,charge
```

## Setting Up Stripe Webhooks

### Authentication: API Key

Stripe does not support custom headers on webhook deliveries. Pass the connector's API key as a query parameter:

```
https://your-connector.example.com?api_key=your-connector-api-key
```

### Via Stripe Dashboard

1. Go to [Stripe Dashboard](https://dashboard.stripe.com) > **Developers** > **Webhooks**
2. Click **Add endpoint**
3. Enter your endpoint URL: `https://your-connector.example.com?api_key=your-connector-api-key`
4. Select events to listen to (or select "All events")
5. Click **Add endpoint**
6. Note the **Signing secret** (`whsec_xxx`) for signature verification

### Via Stripe API

```bash
curl https://api.stripe.com/v1/webhook_endpoints \
  -u sk_live_YOUR_SECRET_KEY: \
  -d url="https://your-connector.example.com?api_key=your-connector-api-key" \
  -d "enabled_events[]"="customer.created" \
  -d "enabled_events[]"="customer.updated" \
  -d "enabled_events[]"="customer.deleted" \
  -d "enabled_events[]"="payment_intent.succeeded" \
  -d "enabled_events[]"="payment_intent.payment_failed" \
  -d "enabled_events[]"="invoice.paid" \
  -d "enabled_events[]"="invoice.finalized" \
  -d "enabled_events[]"="charge.succeeded" \
  -d "enabled_events[]"="charge.refunded" \
  -d "enabled_events[]"="product.created" \
  -d "enabled_events[]"="product.updated"
```

Or subscribe to all events:
```bash
curl https://api.stripe.com/v1/webhook_endpoints \
  -u sk_live_YOUR_SECRET_KEY: \
  -d url="https://your-connector.example.com?api_key=your-connector-api-key" \
  -d "enabled_events[]"="*"
```

### Via Stripe CLI (for testing)

```bash
stripe listen --forward-to http://localhost:8083/webhook?api_key=your-key
```

## Snapshot (Backfill)

The connector supports snapshot backfill using Stripe's REST List APIs with cursor-based pagination (100 items per page).

### Snapshot Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.snapshot.stripe.api.key` | Password | `""` | Stripe API secret key (`sk_live_xxx` or `sk_test_xxx`) |
| `camel.source.snapshot.mode` | String | `no_data` | `initial`, `initial_only`, or `no_data` |
| `camel.source.snapshot.objects` | String | `""` | Objects to snapshot (e.g., `customer,charge,invoice`) |

**Authentication**: Stripe API keys don't expire. No refresh logic needed.

### Supported Snapshot Objects

| Object | API Endpoint | Key |
|---|---|---|
| `customer` | `/v1/customers` | `id` |
| `charge` | `/v1/charges` | `id` |
| `payment_intent` | `/v1/payment_intents` | `id` |
| `invoice` | `/v1/invoices` | `id` |
| `subscription` | `/v1/subscriptions` | `id` |
| `product` | `/v1/products` | `id` |
| `price` | `/v1/prices` | `id` |
| `payout` | `/v1/payouts` | `id` |
| `refund` | `/v1/refunds` | `id` |
| `payment_method` | `/v1/payment_methods` | `id` |
| `balance_transaction` | `/v1/balance_transactions` | `id` |
| `coupon` | `/v1/coupons` | `id` |
| `plan` | `/v1/plans` | `id` |
| `setup_intent` | `/v1/setup_intents` | `id` |

Snapshot records include `__changeType: "SNAPSHOT"`, `__deleted: false`, and `__op: "r"` (header).

### Signal Table

```properties
camel.source.snapshot.signal.topic=stripe_snapshot_signals
```

```json
{
  "id": "backfill-customers",
  "type": "execute-snapshot",
  "data": {
    "data-collections": ["customer", "charge", "invoice"],
    "type": "INCREMENTAL"
  }
}
```

### Filtered Snapshots

Use `additional_condition` with Stripe filter parameters:

```json
{
  "id": "backfill-recent-charges",
  "type": "execute-snapshot",
  "data": {
    "data-collections": ["charge"],
    "type": "INCREMENTAL",
    "additional_condition": "created[gt]=1704067200"
  }
}
```

Supported filter syntax (Stripe query parameters):
- `created[gt]=1704067200` — objects created after a Unix timestamp
- `created[gte]=1704067200` — created at or after
- `status=active` — active subscriptions only
- `customer=cus_xxx` — objects for a specific customer

### Pagination Details

Stripe limits list requests to 100 items per page. The chunk reader pages internally when `chunk.size > 100`. Stripe IDs are string-based (`cus_xxx`, `pi_xxx`), not numeric, so parallel segment splitting is not available.

### Example Snapshot Configuration

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=stripe
camel.source.payload.router.topic.prefix=stripe_

camel.source.snapshot.mode=no_data
camel.source.snapshot.stripe.api.key=sk_live_xxxxx
camel.source.snapshot.signal.topic=stripe_snapshot_signals
camel.source.snapshot.chunk.size=500
```

## Example Configuration

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=stripe
camel.source.payload.router.topic.prefix=stripe_
camel.source.payload.router.include.event=true
camel.source.payload.router.stripe.signing.secret=whsec_xxxxx

topic.include.list.user.defined=customer,payment_intent,invoice,charge

camel.source.snapshot.stripe.api.key=sk_live_xxxxx
camel.source.snapshot.signal.topic=stripe_snapshot_signals

camel.source.dlq.enabled=true
camel.source.dlq.topic=stripe_dlq
```

## Testing with curl

```bash
curl -X POST "http://localhost:8083/webhook?api_key=your-connector-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt_test123",
    "object": "event",
    "api_version": "2023-10-16",
    "created": 1680064028,
    "livemode": false,
    "type": "customer.created",
    "data": {
      "object": {
        "id": "cus_test123",
        "email": "test@example.com",
        "name": "Test Customer"
      }
    }
  }'
```

## Important Notes

- **No guaranteed ordering**: Stripe does not guarantee event ordering. Use `_event_created` timestamp for ordering.
- **Duplicates possible**: Deduplicate on `_event_id` (the event object ID, not `data.object.id`).
- **Respond quickly**: Stripe expects a 2xx response promptly. The connector responds immediately and processes asynchronously.
- **Retry behavior**: Exponential backoff up to 3 days (live mode), 3 attempts (sandbox).
- **Secret rotation**: During rotation, Stripe sends signatures for both old and new secrets. The connector checks all `v1` signatures.
- **Wildcard events**: You can subscribe to `charge.*` in the Dashboard to capture all charge events.
