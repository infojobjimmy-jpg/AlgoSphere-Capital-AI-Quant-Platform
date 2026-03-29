# External Runner Heartbeat

## Heartbeat Model (Optional)
The current API does not require a dedicated heartbeat endpoint. A runner can implement implicit heartbeat via periodic reads (`GET /runner/status`) and local liveness reporting.

## Recommended Heartbeat Interval
- Runner internal heartbeat: every 10 seconds.
- External observability export (logs/metrics): every 30-60 seconds.

## Stale Job Interpretation
A job may be considered stale when:
- It remains `RUNNER_ACKNOWLEDGED` or `RUNNER_ACTIVE` beyond expected SLA.
- No local runner heartbeat is observed for a configured timeout window.

Suggested stale thresholds:
- Acknowledged stale: > 2 minutes without transition.
- Active stale: threshold aligned to simulation expected runtime.

## Operator Guidance for Missing Heartbeat
- If heartbeat missing but API reachable:
  - suspect runner process crash or connectivity loss.
- If heartbeat missing and API unreachable:
  - suspect environment/network outage.

## Recovery Pattern
- Reconcile with `/runner/status`.
- Re-fetch `/runner/jobs`.
- Resume or fail stale jobs based on policy and human/operator decision.
