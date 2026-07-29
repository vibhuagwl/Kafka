# DLQ Reprocess (Dead Letter → Original Topic)

## Flow

```
Consumer fails (retries exhausted / poison)
        ↓
DefaultErrorHandler → DeadLetterPublishingRecoverer
        ↓
Produce to topic `dead-letter` (+ X-Original-*, failure headers)
        ↓
Ops fixes root cause
        ↓
POST /api/v1/dlq/reprocess  { partition, offset, force }
        ↓
Seek DLQ → read record → KafkaTemplate.send(originalTopic)
        ↓
Downstream consumers process again (force clears idempotency)
```

## APIs (replay-service :8088)

```bash
# Inspect
curl "localhost:8088/api/v1/dlq/messages?partition=0&offset=0"
curl "localhost:8088/api/v1/dlq/messages/scan?partition=0&fromOffset=0&max=20"

# Reprocess one (force=true clears Redis/local idempotency for eventId)
curl -X POST localhost:8088/api/v1/dlq/reprocess \
  -H 'Content-Type: application/json' \
  -d '{"partition":0,"offset":0,"force":true}'

# Reprocess range
curl -X POST localhost:8088/api/v1/dlq/reprocess/range \
  -H 'Content-Type: application/json' \
  -d '{"partition":0,"fromOffset":0,"toOffset":5,"force":true}'
```

## Headers added on reprocess

| Header | Meaning |
|--------|---------|
| `X-Reprocessed-From-Dlq` | `true` |
| `X-Reprocess-Id` | Unique reprocess operation id |
| `X-Reprocess-Count` | How many times this payload left DLQ |
| `X-Force-Reprocess` | Clear/bypass idempotency |
| `X-Dlq-Partition` / `X-Dlq-Offset` | Provenance |

## Interview notes

1. **Why not auto-reprocess?** Poison stays poison until code/data is fixed.
2. **force=true** vs **force=false** — without force, inventory may skip as duplicate.
3. **Ordering** — republish uses same key → same partition (ordering preserved for that aggregate).
4. **DLQ message stays** — we don't delete from DLQ (Kafka log is append-only); track via reprocess headers / audit.
5. **Spring internals** — recoverer ProduceRequest to `dead-letter`; reprocess is another ProduceRequest to original topic.
