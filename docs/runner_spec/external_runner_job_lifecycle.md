# External Runner Job Lifecycle

## Lifecycle Summary
Runner jobs move through a controlled progression managed by API state transitions.

1. **Eligible**
   - Job appears in `GET /runner/jobs`.
   - Typical prerequisite: `executor_status == EXECUTOR_READY`.

2. **Acknowledged**
   - Runner claims job via `POST /runner/ack`.
   - Ownership is represented by `runner_id`.

3. **Active**
   - Runner starts processing via `POST /runner/start`.
   - Job is now in active execution workflow (still demo/simulation only).

4. **Paused (Optional)**
   - Runner temporarily pauses via `POST /runner/pause`.
   - Can be resumed to active later.

5. **Terminal**
   - Success path: `POST /runner/complete` -> `RUNNER_COMPLETED`.
   - Failure path: `POST /runner/fail` -> `RUNNER_FAILED`.

## Operational Notes
- Acknowledge before start.
- Use pause for transient issues (network, dependency, resource contention).
- Use fail only for non-recoverable or policy-required aborts.
- Always include concise notes for auditability.
