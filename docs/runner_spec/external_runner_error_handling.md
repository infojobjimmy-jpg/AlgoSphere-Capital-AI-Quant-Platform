# External Runner Error Handling

## Malformed Payload Handling
- Validate required fields before API calls (`strategy_id`, `runner_id` where required).
- Reject invalid local payloads early and log structured error context.
- Do not send partial/ambiguous requests.

## State Mismatch Handling
- Example: `start` called before `ack`.
- Expected behavior:
  1. Read current state (`/runner/jobs` or `/runner/status`).
  2. Reconcile lifecycle.
  3. Re-run the correct transition only when valid.

## Failure Path
- Use `POST /runner/fail` for terminal failures with explicit note.
- Include concise root-cause signal in note:
  - validation error
  - adapter timeout
  - simulator exception
  - policy block

## Duplicate `runner_id` Usage
- Duplicate IDs are possible in distributed setups.
- Safe practice:
  - use unique runner instance ID per process/host.
  - include host + process + startup timestamp suffix.
- On collision suspicion, pause processing and reconcile ownership.

## Safe Defaults
- Default to non-terminal pause before failure when uncertain.
- Prefer explicit notes over silent transitions.
- Never auto-upgrade job states without validating current API state.
- Keep broker-related actions permanently disabled in runner bridge context.
