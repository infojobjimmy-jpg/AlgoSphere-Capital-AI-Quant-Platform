# External Runner State Machine

## States
- `RUNNER_PENDING`
- `RUNNER_ACKNOWLEDGED`
- `RUNNER_ACTIVE`
- `RUNNER_PAUSED`
- `RUNNER_COMPLETED`
- `RUNNER_FAILED`

## Text Diagram
```text
                (eligible from bridge)
                       |
                       v
                RUNNER_PENDING
                       |
                 POST /runner/ack
                       v
            RUNNER_ACKNOWLEDGED
                       |
                POST /runner/start
                       v
                 RUNNER_ACTIVE
                  /         \
POST /runner/pause           POST /runner/complete
        v                              v
  RUNNER_PAUSED                 RUNNER_COMPLETED
        |
POST /runner/start
        v
  RUNNER_ACTIVE

From ACKNOWLEDGED / ACTIVE / PAUSED:
POST /runner/fail -> RUNNER_FAILED
```

## Transition Rules
- Start requires acknowledged or paused state.
- Complete typically requires active or paused state.
- Fail can be used as terminal fallback from non-terminal states.
- Completed/failed states are terminal for current run context.
