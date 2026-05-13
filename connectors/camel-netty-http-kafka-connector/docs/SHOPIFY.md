# Shopify Configuration

Supported when `camel.source.payload.router.type=shopify`.

Unlike Zendesk and Salesforce (which carry event metadata in the JSON body), Shopify sends the event type in the `X-Shopify-Topic` HTTP header. The connector automatically reads this header from the incoming HTTP request.

## How It Works

Shopify sends webhooks as HTTP POST requests with these headers:

| Header | Description | Example |
|--------|-------------|---------|
| `X-Shopify-Topic` | Event type | `orders/create`, `products/update` |
| `X-Shopify-Shop-Domain` | Shop identifier | `myshop.myshopify.com` |
| `X-Shopify-Hmac-Sha256` | HMAC signature | Base64-encoded HMAC-SHA256 |
| `X-Shopify-Event-Id` | Unique event ID (for dedup) | `evt-abc123` |
| `X-Shopify-Triggered-At` | Event timestamp | `2026-05-11T10:00:00Z` |
| `X-Shopify-API-Version` | API version | `2024-01` |
| `X-Shopify-Webhook-Id` | Webhook subscription ID | `wh-456` |

The JSON body is the raw resource (order, product, customer, etc.) with no wrapper envelope.

## Resource Topics

The connector parses the `X-Shopify-Topic` header to determine the resource type and routes to `{prefix}{resource}`:

| Shopify Topic | Generated Kafka Topic | Key |
|---|---|---|
| `orders/create` | `{prefix}orders` | `{id: <order_id>}` |
| `orders/updated` | `{prefix}orders` | `{id: <order_id>}` |
| `orders/delete` | `{prefix}orders` | `{id: <order_id>}` |
| `orders/cancelled` | `{prefix}orders` | `{id: <order_id>}` |
| `orders/fulfilled` | `{prefix}orders` | `{id: <order_id>}` |
| `orders/paid` | `{prefix}orders` | `{id: <order_id>}` |
| `products/create` | `{prefix}products` | `{id: <product_id>}` |
| `products/update` | `{prefix}products` | `{id: <product_id>}` |
| `products/delete` | `{prefix}products` | `{id: <product_id>}` |
| `customers/create` | `{prefix}customers` | `{id: <customer_id>}` |
| `customers/update` | `{prefix}customers` | `{id: <customer_id>}` |
| `customers/delete` | `{prefix}customers` | `{id: <customer_id>}` |
| `draft_orders/create` | `{prefix}draft_orders` | `{id: <draft_order_id>}` |
| `fulfillments/create` | `{prefix}fulfillments` | `{id: <fulfillment_id>}` |
| `inventory_items/update` | `{prefix}inventory_items` | `{id: <item_id>}` |
| `collections/create` | `{prefix}collections` | `{id: <collection_id>}` |
| `shop/update` | `{prefix}shop` | (no key) |
| `customer.tags_added` | `{prefix}customer` | `{id: <customer_id>}` |

## Change Type Mapping

Each record gets `__changeType` and `__deleted` in the value, and `__op` as a Kafka header:

| Shopify Action | `__changeType` | `__op` (header) | `__deleted` |
|---|---|---|---|
| `create` | `CREATE` | `c` | `false` |
| `update` / `updated` | `UPDATE` | `u` | `false` |
| `delete` | `DELETE` | `d` | `true` |
| `cancelled` | `CANCELLED` | `u` | `false` |
| `fulfilled` | `FULFILLED` | `u` | `false` |
| `paid` | `PAID` | `u` | `false` |
| `partially_fulfilled` | `PARTIALLY_FULFILLED` | `u` | `false` |
| Any other action | `ACTION.toUpperCase()` | `u` | `false` |
| Snapshot | `SNAPSHOT` | `r` | `false` |

## Event Metadata

When `camel.source.payload.router.include.event=true` (default), these fields are added to the payload:

| Field | Source |
|---|---|
| `_shop_domain` | `X-Shopify-Shop-Domain` header |
| `_event_id` | `X-Shopify-Event-Id` header |
| `_triggered_at` | `X-Shopify-Triggered-At` header |
| `_api_version` | `X-Shopify-API-Version` header |
| `_webhook_id` | `X-Shopify-Webhook-Id` header |

Set `include.event=false` to omit metadata (useful for upsert/state-table sinks).

## Fan-Out

Shopify resources contain nested arrays that can be fanned out to separate topics:

| Fan-out Config | Source Array | Generated Topic | Key |
|---|---|---|---|
| `orders.line_items` | Order line items | `{prefix}orders_line_items` | `{id: order_id, item_id: line_item_id}` |
| `orders.shipping_lines` | Shipping lines | `{prefix}orders_shipping_lines` | `{id: order_id, item_id: shipping_line_id}` |
| `orders.discount_codes` | Discount codes | `{prefix}orders_discount_codes` | `{id: order_id}` |
| `orders.tax_lines` | Tax lines | `{prefix}orders_tax_lines` | `{id: order_id}` |
| `orders.fulfillments` | Fulfillments | `{prefix}orders_fulfillments` | `{id: order_id, item_id: fulfillment_id}` |
| `products.variants` | Product variants | `{prefix}products_variants` | `{id: product_id, item_id: variant_id}` |
| `products.images` | Product images | `{prefix}products_images` | `{id: product_id, item_id: image_id}` |
| `products.options` | Product options | `{prefix}products_options` | `{id: product_id, item_id: option_id}` |
| `customers.addresses` | Customer addresses | `{prefix}customers_addresses` | `{id: customer_id, item_id: address_id}` |
| `draft_orders.line_items` | Draft order items | `{prefix}draft_orders_line_items` | `{id: draft_order_id, item_id: line_item_id}` |
| `fulfillments.line_items` | Fulfillment items | `{prefix}fulfillments_line_items` | `{id: fulfillment_id, item_id: line_item_id}` |

Fan-out records include `_ctx_event_id` and `_ctx_shop_domain` for correlation with the parent record.

## HMAC Signature Verification

Optional. Verifies that webhook payloads are genuinely from Shopify using HMAC-SHA256:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.payload.router.shopify.hmac.secret` | Password | `""` | Your Shopify app's client secret. When set, verifies every webhook against `X-Shopify-Hmac-Sha256`. Leave empty to disable. |

When enabled, payloads with invalid or missing HMAC signatures are rejected and sent to the DLQ (if configured).

## Allowed Objects Filter

Use `topic.include.list.user.defined` to restrict which Shopify resource types are processed:

```properties
topic.include.list.user.defined=orders,products,customers
```

Events for other resource types are routed to the default topic or skipped based on `unknown.type.behavior`.

## Example Configuration

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=shopify
camel.source.payload.router.topic.prefix=shopify_
camel.source.payload.router.include.event=true
camel.source.payload.router.fanout.fields=orders.line_items,products.variants,customers.addresses
camel.source.payload.router.shopify.hmac.secret=shpss_abc123def456
topic.include.list.user.defined=orders,products,customers
```

## Setting Up Shopify Webhooks

### Authentication: API Key

Shopify does **not** support custom headers on webhook deliveries — it only sends its own `X-Shopify-*` headers. Since our connector requires an API key (`x-api-key` header or `api_key` query parameter), you must pass the API key in the webhook endpoint URL as a query parameter:

```
https://your-connector.example.com?api_key=your-connector-api-key
```

Shopify will POST to this exact URL including the query string, and the connector will authenticate the request via the `api_key` parameter.

For additional security, enable HMAC verification (`camel.source.payload.router.shopify.hmac.secret`) to cryptographically verify that each webhook genuinely came from Shopify.

### Step 1: Create an App in Dev Dashboard

> **Note**: As of January 2026, Shopify deprecated legacy custom apps. New apps must be created via the [Dev Dashboard](https://partners.shopify.com). Legacy custom apps with permanent tokens still work but cannot be created on new stores.

1. Go to the [Shopify Partners Dashboard](https://partners.shopify.com) (create a partner account if needed)
2. Click **Apps** > **Create app**
3. Choose **Create app manually**
4. Name it (e.g., `Kafka Connector`), set the App URL to your connector endpoint
5. Click **Create app**

### Step 2: Configure API Scopes

1. In your app, go to **Configuration**
2. Under **Access scopes**, select the scopes based on which webhook topics you need:

| Webhook Topics | Required Scope |
|---|---|
| `orders/create`, `orders/updated`, `orders/delete`, `orders/cancelled`, `orders/fulfilled`, `orders/paid` | `read_orders` |
| `products/create`, `products/update`, `products/delete` | `read_products` |
| `customers/create`, `customers/update`, `customers/delete` | `read_customers` |
| `draft_orders/create`, `draft_orders/update`, `draft_orders/delete` | `read_draft_orders` |
| `fulfillments/create`, `fulfillments/update` | `read_orders` |
| `inventory_items/create`, `inventory_items/update`, `inventory_items/delete` | `read_inventory` |
| `inventory_levels/connect`, `inventory_levels/update`, `inventory_levels/disconnect` | `read_inventory` |
| `collections/create`, `collections/update`, `collections/delete` | `read_products` |
| `refunds/create` | `read_orders` |
| `themes/create`, `themes/update`, `themes/delete`, `themes/publish` | `read_themes` |

4. Click **Save**

> **Tip**: For a full CDC-style setup, select `read_orders`, `read_products`, `read_customers`, and `read_inventory` at minimum.

### Step 3: Install the App and Get Credentials

1. In your app, go to **Settings**
2. Note the **Client ID** and **Client Secret**
3. Install the app on your store:
   - Go to your store's admin
   - **Settings** > **Apps and sales channels** > **Develop apps** (or install from Partner Dashboard)
   - Install your app and approve the scopes

**Getting an access token** — two options:

**Option A: Client Credentials Grant (recommended for Dev Dashboard apps)**

Tokens auto-refresh every 24 hours. Use these connector config properties:
```properties
camel.source.snapshot.shopify.store.url=https://yourstore.myshopify.com
camel.source.snapshot.shopify.client.id=YOUR_CLIENT_ID
camel.source.snapshot.shopify.client.secret=YOUR_CLIENT_SECRET
```

The connector handles token acquisition and refresh automatically.

To get a token manually for webhook registration (Step 4), use curl:
```bash
curl -X POST "https://YOUR-STORE.myshopify.com/admin/oauth/access_token" \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "grant_type": "client_credentials"
  }'
```

The response contains `access_token` (valid for 24 hours).

**Option B: Static Access Token (legacy custom apps only)**

If you have an existing legacy custom app with a permanent token:
```properties
camel.source.snapshot.shopify.store.url=https://yourstore.myshopify.com
camel.source.snapshot.shopify.access.token=shpat_XXXXX
```

### Step 4: Register Webhooks via GraphQL Admin API

The Admin UI only lets you create webhooks one at a time. For many topics, use the GraphQL Admin API. You can run these from any HTTP client (curl, Postman, Insomnia).

**Base URL**: `https://YOUR-STORE.myshopify.com/admin/api/2024-10/graphql.json`

**Required header**: `X-Shopify-Access-Token: YOUR_ACCESS_TOKEN`

#### Create a single webhook

```bash
curl -X POST "https://YOUR-STORE.myshopify.com/admin/api/2024-10/graphql.json" \
  -H "Content-Type: application/json" \
  -H "X-Shopify-Access-Token: YOUR_ACCESS_TOKEN" \
  -d '{
    "query": "mutation { webhookSubscriptionCreate(topic: ORDERS_CREATE, webhookSubscription: { callbackUrl: \"https://your-connector.example.com?api_key=your-connector-api-key\", format: JSON }) { webhookSubscription { id } userErrors { field message } } }"
  }'
```

#### Create all common webhooks at once

Use a shell script to register multiple topics in one go:

```bash
#!/bin/bash
STORE="YOUR-STORE.myshopify.com"
TOKEN="YOUR_ACCESS_TOKEN"
CALLBACK="https://your-connector.example.com?api_key=your-connector-api-key"
API_VERSION="2024-10"

TOPICS=(
  # Orders
  ORDERS_CREATE
  ORDERS_UPDATED
  ORDERS_DELETE
  ORDERS_CANCELLED
  ORDERS_FULFILLED
  ORDERS_PAID
  # Products
  PRODUCTS_CREATE
  PRODUCTS_UPDATE
  PRODUCTS_DELETE
  # Customers
  CUSTOMERS_CREATE
  CUSTOMERS_UPDATE
  CUSTOMERS_DELETE
  # Draft Orders
  DRAFT_ORDERS_CREATE
  DRAFT_ORDERS_UPDATE
  DRAFT_ORDERS_DELETE
  # Fulfillments
  FULFILLMENTS_CREATE
  FULFILLMENTS_UPDATE
  # Inventory
  INVENTORY_ITEMS_CREATE
  INVENTORY_ITEMS_UPDATE
  INVENTORY_ITEMS_DELETE
  INVENTORY_LEVELS_CONNECT
  INVENTORY_LEVELS_UPDATE
  INVENTORY_LEVELS_DISCONNECT
  # Collections
  COLLECTIONS_CREATE
  COLLECTIONS_UPDATE
  COLLECTIONS_DELETE
)

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating webhook for $TOPIC..."
  curl -s -X POST "https://$STORE/admin/api/$API_VERSION/graphql.json" \
    -H "Content-Type: application/json" \
    -H "X-Shopify-Access-Token: $TOKEN" \
    -d "{
      \"query\": \"mutation { webhookSubscriptionCreate(topic: $TOPIC, webhookSubscription: { callbackUrl: \\\"$CALLBACK\\\", format: JSON }) { webhookSubscription { id } userErrors { field message } } }\"
    }" | python3 -c "import sys,json; r=json.load(sys.stdin); d=r.get('data',{}).get('webhookSubscriptionCreate',{}); errs=d.get('userErrors',[]); print(f'  OK: {d.get(\"webhookSubscription\",{}).get(\"id\",\"?\")}') if not errs else print(f'  ERROR: {errs}')"
done

echo "Done. Total topics: ${#TOPICS[@]}"
```

Save as `setup-shopify-webhooks.sh`, edit the variables at the top, then run:

```bash
chmod +x setup-shopify-webhooks.sh
./setup-shopify-webhooks.sh
```

#### Verify registered webhooks

List all active webhook subscriptions:

```bash
curl -s -X POST "https://YOUR-STORE.myshopify.com/admin/api/2024-10/graphql.json" \
  -H "Content-Type: application/json" \
  -H "X-Shopify-Access-Token: YOUR_ACCESS_TOKEN" \
  -d '{
    "query": "{ webhookSubscriptions(first: 50) { edges { node { id topic endpoint { ... on WebhookHttpEndpoint { callbackUrl } } } } } }"
  }'
```

#### Delete a webhook

```bash
curl -s -X POST "https://YOUR-STORE.myshopify.com/admin/api/2024-10/graphql.json" \
  -H "Content-Type: application/json" \
  -H "X-Shopify-Access-Token: YOUR_ACCESS_TOKEN" \
  -d '{
    "query": "mutation { webhookSubscriptionDelete(id: \"gid://shopify/WebhookSubscription/1234567890\") { deletedWebhookSubscriptionId userErrors { field message } } }"
  }'
```

### Via Shopify Admin UI (Simple Setup)

For a quick setup with fewer topics, you can use the UI instead:

1. In Shopify Admin, go to **Settings** > **Notifications** > **Webhooks**
2. Click **Create webhook**
3. Select the event (e.g., `Order creation`)
4. Set format to **JSON**
5. Enter your connector URL **with the API key as a query parameter**:
   `https://your-connector.example.com?api_key=your-connector-api-key`
6. Save
7. Repeat for each event

### Webhook Topic Reference

Full list of GraphQL enum values for `webhookSubscriptionCreate`:

| Category | Topics |
|---|---|
| Orders | `ORDERS_CREATE`, `ORDERS_UPDATED`, `ORDERS_DELETE`, `ORDERS_CANCELLED`, `ORDERS_EDITED`, `ORDERS_FULFILLED`, `ORDERS_PAID`, `ORDERS_PARTIALLY_FULFILLED` |
| Products | `PRODUCTS_CREATE`, `PRODUCTS_UPDATE`, `PRODUCTS_DELETE` |
| Customers | `CUSTOMERS_CREATE`, `CUSTOMERS_UPDATE`, `CUSTOMERS_DELETE`, `CUSTOMERS_EMAIL_MARKETING_CONSENT_UPDATE` |
| Draft Orders | `DRAFT_ORDERS_CREATE`, `DRAFT_ORDERS_UPDATE`, `DRAFT_ORDERS_DELETE` |
| Fulfillments | `FULFILLMENTS_CREATE`, `FULFILLMENTS_UPDATE` |
| Inventory | `INVENTORY_ITEMS_CREATE`, `INVENTORY_ITEMS_UPDATE`, `INVENTORY_ITEMS_DELETE`, `INVENTORY_LEVELS_CONNECT`, `INVENTORY_LEVELS_UPDATE`, `INVENTORY_LEVELS_DISCONNECT` |
| Collections | `COLLECTIONS_CREATE`, `COLLECTIONS_UPDATE`, `COLLECTIONS_DELETE` |
| Refunds | `REFUNDS_CREATE` |
| App | `APP_UNINSTALLED`, `APP_SUBSCRIPTIONS_UPDATE` |
| Shop | `SHOP_UPDATE` |
| Compliance | `CUSTOMERS_DATA_REQUEST`, `CUSTOMERS_REDACT`, `SHOP_REDACT` |

See [WebhookSubscriptionTopic enum](https://shopify.dev/docs/api/admin-graphql/latest/enums/WebhookSubscriptionTopic) for the complete list.

### Troubleshooting

| Issue | Check |
|---|---|
| "Access denied" on webhook creation | Verify the app has the correct scopes (Step 2). Reinstall the app after changing scopes. |
| Webhook not firing | Check **Settings** > **Notifications** > **Webhooks** to see delivery status. Shopify retries 8 times over 4 hours. |
| Webhook deleted automatically | After 8 consecutive delivery failures, Shopify removes the subscription. Fix connectivity, then re-register. |
| "Callback URL is not allowed" | URL must be HTTPS in production. For testing, use a tunnel (ngrok, Cloudflare Tunnel). |
| Events missing in Kafka | Verify the topic is registered (`webhookSubscriptions` query). Check connector logs and DLQ. |
| Protected customer data error | In Partner Dashboard > your app > **API access** > **Protected customer data access**, request access and fill out the form. |

## Testing with curl

```bash
curl -X POST "http://localhost:8083/webhook?api_key=your-connector-api-key" \
  -H "Content-Type: application/json" \
  -H "X-Shopify-Topic: orders/create" \
  -H "X-Shopify-Shop-Domain: testshop.myshopify.com" \
  -H "X-Shopify-Event-Id: evt-123" \
  -H "X-Shopify-Triggered-At: 2026-05-11T10:00:00Z" \
  -d '{"id": 12345, "email": "customer@example.com", "total_price": "29.99", "line_items": [{"id": 1, "title": "Widget", "quantity": 2}]}'
```

## Snapshot (Backfill)

The connector supports Debezium-style initial and signal-triggered snapshots for Shopify using the GraphQL Admin API with cursor-based pagination.

### Snapshot Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `camel.source.snapshot.mode` | String | `no_data` | `initial` = snapshot on first run then webhooks. `initial_only` = snapshot then stop. `no_data` = webhooks only. |
| `camel.source.snapshot.objects` | String | `""` | Objects to snapshot (e.g., `orders,products,customers`) |
| `camel.source.snapshot.shopify.store.url` | String | `""` | Store URL (e.g., `https://mystore.myshopify.com`) |
| `camel.source.snapshot.shopify.access.token` | Password | `""` | Static access token (legacy custom apps only) |
| `camel.source.snapshot.shopify.client.id` | String | `""` | Client ID from Dev Dashboard (recommended) |
| `camel.source.snapshot.shopify.client.secret` | Password | `""` | Client Secret from Dev Dashboard (recommended) |
| `camel.source.snapshot.shopify.api.version` | String | `2024-10` | API version |

**Authentication**: Provide either `access.token` (legacy) OR `client.id` + `client.secret` (Dev Dashboard). Client credentials tokens auto-refresh every 24 hours.

### Supported Snapshot Objects

| Object | GraphQL Connection | Fields Included |
|---|---|---|
| `orders` | `orders` | id, name, email, createdAt, updatedAt, totalPriceSet (amount + currency), displayFinancialStatus, displayFulfillmentStatus, cancelledAt, closedAt, customer (id + email), lineItems (id, title, quantity, sku, price) |
| `products` | `products` | id, title, handle, status, vendor, productType, createdAt, updatedAt, variants (id, title, sku, price, inventoryQuantity), images (id, url, altText) |
| `customers` | `customers` | id, firstName, lastName, email, phone, createdAt, updatedAt, state, numberOfOrders, addresses (address1, address2, city, province, country, zip) |
| `draft_orders` | `draftOrders` | id, name, status, createdAt, updatedAt, lineItems (id, title, quantity, price) |
| `collections` | `collections` | id, title, handle, updatedAt, sortOrder |
| `inventory_items` | `inventoryItems` | id, sku, createdAt, updatedAt, requiresShipping, tracked |

Snapshot records include `__changeType: "SNAPSHOT"`, `__deleted: false`, and `__op: "r"` as a Kafka header.

Fan-out also works for snapshot records. If `products.variants` is configured in `fanout.fields`, each product snapshot produces the main product record + one record per variant in `{prefix}products_variants`.

### Signal Table (On-Demand Snapshots)

Configure a Kafka signal topic for ad-hoc snapshots (same as Debezium):

```properties
camel.source.snapshot.signal.topic=shopify_snapshot_signals
```

Then send a signal message to trigger a snapshot. Both Debezium-compatible (`data-collections`) and our format (`objects`) are supported:

```json
{
  "id": "backfill-orders-2024",
  "type": "execute-snapshot",
  "data": {
    "data-collections": ["orders", "products", "customers"],
    "type": "INCREMENTAL"
  }
}
```

Or equivalently:

```json
{
  "id": "backfill-orders-2024",
  "type": "execute-snapshot",
  "data": {
    "objects": ["orders", "products", "customers"],
    "type": "INCREMENTAL"
  }
}
```

Other signal types: `stop-snapshot`, `pause-snapshot`, `resume-snapshot`.

### Filtered Snapshots

Use `additional_condition` to snapshot a subset of data. The condition is passed as a Shopify `query` filter:

```json
{
  "id": "backfill-recent-orders",
  "type": "execute-snapshot",
  "data": {
    "data-collections": ["orders"],
    "type": "INCREMENTAL",
    "additional_condition": "created_at:>2024-01-01"
  }
}
```

Supported filter syntax (Shopify query language):
- `created_at:>2024-01-01` — orders created after a date
- `updated_at:>2024-06-01` — recently updated records
- `financial_status:paid` — paid orders only
- `status:active` — active products only
- `created_at:>2024-01-01 AND financial_status:paid` — combine filters with AND

### Pagination Details

Shopify limits GraphQL connections to 250 items per page. The chunk reader pages internally when `chunk.size > 250` (e.g., chunk size 1000 = 4 internal API calls per chunk). Shopify GIDs (`gid://shopify/Order/12345`) are converted to numeric IDs for consistent keying.

### Example Snapshot Configuration

```properties
camel.source.payload.router.enabled=true
camel.source.payload.router.type=shopify
camel.source.payload.router.topic.prefix=shopify_

# Snapshot: initial load then switch to webhooks
camel.source.snapshot.mode=initial
camel.source.snapshot.objects=orders,products,customers
camel.source.snapshot.shopify.store.url=https://mystore.myshopify.com

# Auth option 1: Dev Dashboard (recommended, auto-refreshes every 24h)
camel.source.snapshot.shopify.client.id=YOUR_CLIENT_ID
camel.source.snapshot.shopify.client.secret=YOUR_CLIENT_SECRET

# Auth option 2: Legacy custom app (permanent token, uncomment if using legacy)
# camel.source.snapshot.shopify.access.token=shpat_xxxxxxxxxxxxx

camel.source.snapshot.shopify.api.version=2024-10

# Signal topic for on-demand snapshots
camel.source.snapshot.signal.topic=shopify_snapshot_signals
camel.source.snapshot.chunk.size=1000
camel.source.snapshot.chunk.delay.ms=500
```

## Important Notes

- **No ordering guarantee**: Shopify does not guarantee event ordering. Use `_triggered_at` or `updated_at` for ordering.
- **Duplicates possible**: Use `_event_id` for deduplication.
- **5-second timeout**: Shopify expects a 200 response within 5 seconds. The connector responds immediately and processes asynchronously.
- **Retry behavior**: 8 retries over 4 hours on failure. After 8 consecutive failures, Shopify deletes the webhook subscription.
- **Mandatory compliance webhooks**: Your app must handle `customers/data_request`, `customers/redact`, and `shop/redact`.
