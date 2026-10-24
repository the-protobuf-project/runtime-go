# Events

Change-feed webhooks. A dispatcher reads a service's changes from its records,
ordered by `update_time` from a stored cursor, records one delivery per webhook
and change, and POSTs each as a signed [CloudEvent](https://cloudevents.io)
until the endpoint accepts it or attempts run out.

Nothing that writes a record knows webhooks exist, and no change is lost
between a write and its announcement: a record's `update_time` and `etag` are
the change log, and a soft delete (AIP-164) is a change like any other.

## Installation

```bash
go get github.com/the-protobuf-project/runtime-go/events
```

## Usage

The service supplies two things: a `Feed` per kind of record, and a `Store`
for webhooks, deliveries and cursors.

```go
feed := events.NewFeed(events.FeedSpec[*orderpb.Order]{
    ID:       "orders",                    // names the cursor; keep it stable
    ReadMask: []string{"name", "etag", "create_time", "update_time", "state"},
    List: func(ctx context.Context, q events.Query) ([]*orderpb.Order, string, error) {
        return orders.List(ctx, toListInput(q)) // filter, order, mask, show_deleted
    },
    Change: func(o *orderpb.Order) events.Change {
        return events.Change{
            Type:  "shop.order." + strings.ToLower(strings.TrimPrefix(o.GetState().String(), "STATE_")),
            Time:  o.GetUpdateTime().AsTime(),
            Scope: o.GetStore(), // matched against Webhook.Scope; "" for none
        }
    },
    Data: func(o *orderpb.Order) ([]byte, error) { return protojson.Marshal(o) },
})

sealer, _ := events.NewSealer(cfg.SecretKey, cfg.PreviousSecretKeys)
d := events.NewDispatcher(store, []events.Feed{feed}, events.Options{
    Source:       "//shop.example.com",
    UserAgent:    "shop-webhooks/1.0",
    MetricPrefix: "shop.webhook",
    Sealer:       sealer,
    Meter:        meter,
})
d.Start(ctx, 5*time.Second, func(err error) { log.Error("webhooks", "error", err) })
```

A record with no state of its own reports its lifecycle:
`events.Lifecycle(create, update, delete)` is `created`, `updated`, `deleted`,
or `""` -- a write to an already-deleted record, sent to nobody.

When a webhook is created, the API layer checks its endpoint and mints its
secret:

```go
if err := events.CheckEndpoint(uri, events.EndpointPolicy{}); err != nil { ... } // *EndpointError{Reason, What, Fix}
secret, _ := events.NewSecret()   // "whsec_…", returned to the caller once
sealed, _ := sealer.Seal(secret)  // what the store keeps
```

## Guarantees

| | |
| --- | --- |
| Late commits | Each round rereads `Lag` (30 s) before the cursor; a seen-set stops a reread recording twice; an idle cursor settles and stops rereading. |
| New webhooks | A round takes changes only up to its read of the webhooks, less `HookSettle` (1 s). |
| Several servers | A delivery's name is derived from (webhook, event), so a change recorded twice collides; a delivery is claimed, etag-guarded, before it is sent. |
| Superseded versions | A change whose record moved on before it was fetched is left to the read of its new version. |
| Retries | 30 s, doubling to 6 h; `FAILED` after `MaxAttempts` (8) or when the webhook was deleted; a paused webhook's deliveries wait. |
| Signing | [Standard Webhooks](https://www.standardwebhooks.com): `webhook-id`, `webhook-timestamp`, `webhook-signature: v1,<base64 HMAC-SHA256>`; after a rotation the previous secret signs too for `RotationGrace` (24 h). `Verify` checks one as a receiver would. |
| Secrets at rest | `Sealer`: AES-256-GCM, `enc:v1:<key id>:…`, current key plus the keys it replaced; unsealed values are read as they are. |
| SSRF | The default client connects only to public addresses (checked on the address DNS returned), follows no redirects, uses no proxy. |
| Retention | Finished deliveries are pruned after `Retention` (30 days). |
| Metrics | `<prefix>.attempts{outcome=succeeded\|retrying\|failed}`, `<prefix>.delivery_lag` (s). |

## Store

`Store` implementations wrap their backend's errors in `events.ErrNotFound`,
`ErrAlreadyExists` and `ErrConflict`. `UpdateDelivery` and `UpdateCursor` must
be guarded by the record's etag: that is what makes claims and cursor moves
safe across servers.
