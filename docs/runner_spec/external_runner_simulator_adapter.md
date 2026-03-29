# External Runner Simulator Adapter

## Objective
Define how a pure simulator runner can consume runner jobs and execute controlled demo simulations.

## Recommended Flow
1. Pull jobs from `GET /runner/jobs`.
2. Ack selected job with `POST /runner/ack`.
3. Start simulation with `POST /runner/start`.
4. Pause/resume as needed with `POST /runner/pause` and `POST /runner/start`.
5. Mark terminal outcome:
   - success via `POST /runner/complete`
   - failure via `POST /runner/fail`

## Simulator Adapter Responsibilities
- Deterministic simulation execution.
- Result and status reporting.
- Safe retries and backoff on transient failures.
- Strict adherence to API state transitions.

## Operational Considerations
- Keep simulation environment reproducible.
- Log seed/version/config metadata for auditability.
- Use stable `runner_id` per simulator instance.

## Safety Constraints
- No broker connectivity required.
- No live order placement.
- No bypass of review/demo/executor controls.
- Workflow orchestration only.
