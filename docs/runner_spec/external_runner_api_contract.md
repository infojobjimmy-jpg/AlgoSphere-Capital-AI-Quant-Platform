# External Runner API Contract

This document defines how an external runner should consume the current Demo Runner Bridge endpoints.

## 1) List Jobs

- **Method**: `GET`
- **Path**: `/runner/jobs`
- **Parameters**:
  - `limit` (query, optional, int): max jobs to return.
- **Example Request**:
  - `GET /runner/jobs?limit=10`
- **Example Response**:
```json
{
  "count": 1,
  "jobs": [
    {
      "strategy_id": "abc123",
      "family": "MEAN_REVERSION",
      "executor_status": "EXECUTOR_READY",
      "demo_status": "DEMO_ASSIGNED",
      "review_status": "APPROVED_FOR_DEMO",
      "runner_status": "",
      "runner_id": "",
      "runner_priority": 26.85,
      "executor_target": "demo_runner_a",
      "eligible": true
    }
  ]
}
```
- **Expected Usage**: poll and choose eligible jobs by priority and target.

## 2) Acknowledge Job

- **Method**: `POST`
- **Path**: `/runner/ack`
- **Parameters**:
  - `strategy_id` (query, required, str)
  - `runner_id` (query, required, str)
  - `note` (query, optional, str)
- **Example Request**:
  - `POST /runner/ack?strategy_id=abc123&runner_id=runner_alpha`
- **Example Response**:
```json
{
  "ok": true,
  "strategy_id": "abc123",
  "runner_status": "RUNNER_ACKNOWLEDGED",
  "runner_id": "runner_alpha",
  "bridge_only": true
}
```
- **Expected Usage**: lock ownership for this runner instance before activation.

## 3) Start Job

- **Method**: `POST`
- **Path**: `/runner/start`
- **Parameters**:
  - `strategy_id` (query, required, str)
  - `note` (query, optional, str)
- **Example Request**:
  - `POST /runner/start?strategy_id=abc123`
- **Example Response**:
```json
{
  "ok": true,
  "strategy_id": "abc123",
  "runner_status": "RUNNER_ACTIVE",
  "bridge_only": true
}
```
- **Expected Usage**: mark transition from acknowledged to active processing.

## 4) Pause Job

- **Method**: `POST`
- **Path**: `/runner/pause`
- **Parameters**:
  - `strategy_id` (query, required, str)
  - `note` (query, optional, str)
- **Example Request**:
  - `POST /runner/pause?strategy_id=abc123&note=network_issue`
- **Example Response**:
```json
{
  "ok": true,
  "strategy_id": "abc123",
  "runner_status": "RUNNER_PAUSED",
  "bridge_only": true
}
```
- **Expected Usage**: temporary halt with resumable state.

## 5) Complete Job

- **Method**: `POST`
- **Path**: `/runner/complete`
- **Parameters**:
  - `strategy_id` (query, required, str)
  - `note` (query, optional, str)
- **Example Request**:
  - `POST /runner/complete?strategy_id=abc123&note=simulation_done`
- **Example Response**:
```json
{
  "ok": true,
  "strategy_id": "abc123",
  "runner_status": "RUNNER_COMPLETED",
  "bridge_only": true
}
```
- **Expected Usage**: terminal success state.

## 6) Fail Job

- **Method**: `POST`
- **Path**: `/runner/fail`
- **Parameters**:
  - `strategy_id` (query, required, str)
  - `note` (query, optional, str)
- **Example Request**:
  - `POST /runner/fail?strategy_id=abc123&note=validation_failed`
- **Example Response**:
```json
{
  "ok": true,
  "strategy_id": "abc123",
  "runner_status": "RUNNER_FAILED",
  "bridge_only": true
}
```
- **Expected Usage**: terminal failure state with diagnostic note.

## 7) Runner Status

- **Method**: `GET`
- **Path**: `/runner/status`
- **Parameters**: none
- **Example Request**:
  - `GET /runner/status`
- **Example Response**:
```json
{
  "counts": {
    "RUNNER_PENDING": 0,
    "RUNNER_ACKNOWLEDGED": 1,
    "RUNNER_ACTIVE": 0,
    "RUNNER_PAUSED": 0,
    "RUNNER_COMPLETED": 5,
    "RUNNER_FAILED": 1
  },
  "current_jobs": [],
  "summary": {
    "total_jobs": 7,
    "active_or_acknowledged": 1,
    "completed": 5,
    "failed": 1
  },
  "bridge_only": true
}
```
- **Expected Usage**: operator and runner health checks, reconciliation, metrics.
