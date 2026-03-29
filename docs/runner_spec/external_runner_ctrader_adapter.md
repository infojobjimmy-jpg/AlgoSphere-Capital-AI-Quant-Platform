# External Runner cTrader Adapter (Future)

## Objective
Describe how a future cTrader-based adapter could consume Demo Runner Bridge jobs without bypassing safety controls.

## Expected Flow
1. Poll `GET /runner/jobs`.
2. Select eligible job targeting cTrader/simulation context.
3. Acknowledge with `POST /runner/ack`.
4. Mark active with `POST /runner/start`.
5. On temporary issue, `POST /runner/pause`.
6. On successful simulation cycle, `POST /runner/complete`.
7. On hard failure, `POST /runner/fail`.

## cTrader Adapter Constraints
- Must remain demo-only.
- Must never place live broker orders from this bridge workflow.
- Must respect review/demo/executor gating already enforced by API.
- Must not mutate strategy governance fields outside runner endpoints.

## Suggested Adapter Responsibilities
- Job polling and filtering.
- Local simulation orchestration.
- Transition reporting back to API.
- Structured logging and retry policy enforcement.

## Safety Guarantee
This adapter is a consumer of pre-approved workflow states only. It is not a privilege escalation path to live execution.
