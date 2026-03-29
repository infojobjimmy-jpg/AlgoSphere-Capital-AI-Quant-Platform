# External Runner Retry Policy

## Polling Frequency
- Default: poll `GET /runner/jobs` every 5-10 seconds.
- Backoff when idle: increase to 15-30 seconds if no eligible jobs.
- Avoid aggressive polling (<2 seconds) to reduce API noise.

## Retry Behavior
- For transient HTTP/network errors:
  - retry with exponential backoff (e.g., 1s, 2s, 4s, 8s).
- For `4xx` contract errors:
  - do not blind-retry; correct payload/state first.
- For `5xx` server errors:
  - retry with bounded backoff and alert on threshold.

## Max Retry Count
- Per API operation: 3 immediate retries.
- After max retries, move to cooldown and emit local warning.

## Cooldown Policy
- Recommended cooldown after repeated failures: 30-120 seconds.
- Continue heartbeat/status polling during cooldown.

## Idempotency Expectations
- Runner should treat operations as idempotent from its side:
  - repeated ack for same job/runner should be safe to reconcile.
  - start/pause/complete/fail should verify current state before retry.
- Maintain local operation log keyed by `strategy_id` + action + timestamp.
