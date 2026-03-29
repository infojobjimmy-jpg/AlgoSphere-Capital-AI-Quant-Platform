# External Runner Security

## Local-First Assumption
Current deployment model is local-first. Runner and API are expected to operate in a controlled local or private network environment.

## Credentials and Secrets
- Do not embed credentials in runner source code.
- Use environment variables or local secret stores if auth is introduced later.
- Avoid hard-coded tokens in scripts and logs.

## Runner Identity
- Use explicit `runner_id` values for traceability.
- Recommended format: `<adapter>-<host>-<instance-id>`.
- Keep IDs stable during process lifetime.

## Safe Network Boundaries
- Bind APIs to trusted interfaces only.
- Restrict inbound access to operator and runner hosts.
- Use firewall rules to block unknown origins.

## Future Authentication Suggestions
- API key with rotation and scoped permissions.
- mTLS for service-to-service trust.
- Signed requests with nonce/timestamp to prevent replay.
- Audit logging for all state transitions.

## Security Non-Goals in Current Scope
- No broker credentials handling.
- No live execution permissions.
- No privileged trading controls.
