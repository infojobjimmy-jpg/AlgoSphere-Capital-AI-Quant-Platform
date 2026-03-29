# External Runner Overview

## Purpose
The External Runner is a separate process that consumes jobs exposed by Algo Sphere Demo Runner Bridge and advances job states in a controlled, demo-only workflow.

## What the Runner Does
- Polls runner jobs from `GET /runner/jobs`.
- Acknowledges selected jobs (`POST /runner/ack`).
- Marks execution progress (`/runner/start`, `/runner/pause`).
- Marks terminal outcomes (`/runner/complete`, `/runner/fail`).
- Reports status by reading `GET /runner/status`.

## Safe Orchestration Role
The runner is an orchestrator of state transitions only. It is not a trading engine and is not authorized to place broker orders through Algo Sphere APIs.

## What the Runner Must Never Do
- Must never perform live trading.
- Must never deploy real money.
- Must never bypass review, demo, or executor gates.
- Must never assume that "runner-ready" means broker-ready.
- Must never mutate unrelated strategy lifecycle fields outside runner endpoints.

## Scope Boundary
- In scope: job pulling, acknowledgement, progress state updates, completion/failure reporting.
- Out of scope: broker connectivity logic inside Algo Sphere API, real execution, portfolio/capital mutation.

## Operating Principle
Treat Algo Sphere as the source of truth for job eligibility and lifecycle constraints. Runner logic should be conservative, idempotent, and failure-tolerant.
