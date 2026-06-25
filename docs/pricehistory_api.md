# Price History API (frontend integration)

`pricehistory` is the fourth tracked stream, alongside `priceoverview`,
`histogram`, and `activity`. From the frontend's perspective it behaves like the
other three: you add it to the tracked set, then read its data. The only
differences are noted inline below.

Base URL (dev): `http://localhost:8000` — CORS allows `http://localhost:3000`
and `http://127.0.0.1:3000`. Interactive docs: `GET /docs`.

> The API process is read + tracked-set writes only. Actual ingestion (the
> hourly fetch) runs in the separate scheduler process (`cerebro.py`). If the
> scheduler isn't running, an added item sits "tracked, collecting" and never
> gets data. Both must be up.

---

## What makes pricehistory different

- **No poll interval.** History updates on a fixed hourly schedule server-side.
  `poll_interval_sec` is **optional** in the add/modify body and **ignored** for
  this stream — don't send a cadence picker for it. (The other three streams
  still require `poll_interval_sec`.)
- **No `item_nameid` needed.** Like `priceoverview`, history resolves by name.
- **Series, not a snapshot.** A read returns the full stored time series, not a
  single latest point.
- **Cadence in `/items`** comes back as `3600` (the hour it runs on); treat it
  as "hourly", not a tunable value.

---

## Add a price-history item

`POST /tracked-items` → `202 Accepted`

```jsonc
// request body
{
  "market_hash_name": "AK-47 | Redline (Field-Tested)",
  "appid": 730,
  "stream": "pricehistory",
  "currency": 1,          // optional, Steam currency id, default 1 (USD)
  "country": "US",        // optional, default "US"
  "language": "english"   // optional, default "english"
  // poll_interval_sec: omit it — ignored for pricehistory
}
```

```jsonc
// 202 response (TrackingAck)
{
  "status": "tracking",
  "market_hash_name": "AK-47 | Redline (Field-Tested)",
  "stream": "pricehistory",
  "note": "collecting first data",
  // Seed payload: the SAME shape as GET /history (see below), so you can prime
  // your cache without an extra round-trip. null on a fresh add (no data yet);
  // populated when re-adding an item that already has stored history.
  "data": null
}
```

`202` means "tracked, will collect" — **not** that data exists yet. Show a
"collecting" state until the first hourly fetch lands (or until `data` / a
follow-up `GET /history` returns points).

### Errors

| Status | When |
|--------|------|
| 400 | empty `market_hash_name`, bad `appid`/`currency`, invalid `stream` |
| 409 | already tracked + enabled on this stream |

> `poll_interval_sec` validation (required + bounds) only applies to the live
> streams. For pricehistory, sending it is harmless (ignored); omitting it is
> the intended path.

---

## Read price history

`GET /history/{market_hash_name}?range=month`

`range` (optional, default `month`): `week` | `month` | `year` | `all`.

```jsonc
// 200 response (HistoryResponse)
{
  "currency": "USD",
  "points": [
    {
      "timestamp": "2026-06-01T13:00:00+00:00", // ISO 8601 UTC
      "currency": "USD",
      "price": 12.34,    // USD major units
      "volume": 87
    }
    // ... oldest first (ascending time)
  ]
}
```

A **tracked-but-empty** item (just added, first hourly fetch not in yet) returns
`200` with an empty payload — same as the other live read endpoints:

```jsonc
{ "currency": null, "points": [] }
```

### Errors

| Status | When |
|--------|------|
| 400 | invalid `range` value |
| 404 | item is not tracked at all |

> Uniform with `/overview`, `/orderbook`, `/activity`: 200 empty when tracked but
> still collecting, 404 only when the item isn't tracked. So for a freshly added
> item, poll the same endpoint and switch from "collecting" to "live" once
> `points` is non-empty — no special 404 handling needed.

URL-encode `market_hash_name` in the path (it contains spaces, `|`, `()`):
`/history/AK-47%20%7C%20Redline%20(Field-Tested)`.

---

## Modify

`PATCH /tracked-items`

```jsonc
{
  "market_hash_name": "AK-47 | Redline (Field-Tested)",
  "stream": "pricehistory",   // target row's current stream (disambiguator)
  "enabled": false            // pause/resume
  // new_stream: "priceoverview"  // optional: move row to another stream
  // poll_interval_sec: ignored while target stream is pricehistory
}
```

Returns `200` with a `TrackingAck` (same shape as the POST response, `data`
seeded from the row's resulting stream).

## Remove (disable)

`DELETE /tracked-items?market_hash_name=...&stream=pricehistory`

Soft-disable (keeps the row + its history). `stream` query param is required
only when the name is tracked on more than one stream (otherwise a `409` asks
for it).

---

## Listing the tracked set

`GET /items` returns every enabled tracked item across all four streams:

```jsonc
[
  {
    "market_hash_name": "AK-47 | Redline (Field-Tested)",
    "appid": 730,
    "item_nameid": null,        // null for pricehistory (not needed)
    "stream": "pricehistory",
    "currency": "USD",          // ISO code
    "poll_interval_sec": 3600   // hourly — display as "hourly", not editable
  }
]
```

Use `stream` to pick the right read endpoint per item:

| stream | read endpoint |
|--------------|------------------------|
| priceoverview | `GET /overview/{name}` |
| histogram | `GET /orderbook/{name}` |
| activity | `GET /activity/{name}` |
| pricehistory | `GET /history/{name}` |
