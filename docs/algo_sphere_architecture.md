# Algo Sphere Architecture

## Architecture Map (Text Diagram)
```text
Operator Console
     ↓
Meta AI
     ↓
Pipeline Engine
     ↓
Factory → Evolution → Paper → Feedback
     ↓
Live Safe → Review Desk → Demo Deploy Desk
     ↓
Portfolio AI
     ↓
Capital Engine
```

## Module Responsibilities

### Bot Factory
Generates strategy candidates across families (EMA_CROSS, MOMENTUM, MEAN_REVERSION, SESSION_BREAKOUT) with parameter variants and initial fitness scoring.

### Evolution Engine
Mutates and recombines high-performing candidates, tracks lineage (`parent_strategy_id`, `generation`, `origin_type`), and keeps outputs in candidate mode only.

### Paper Trading
Deploys selected candidates into simulation-only paper mode, stores paper metrics (profit, drawdown, win rate, trades), and never executes on broker.

### Feedback Engine
Transforms paper outcomes into `feedback_score`, `promotion_score`, and lifecycle actions (`PROMOTE`, `REJECT`, `EVOLVE_AGAIN`).

### Live Safe Mode
Applies strict promotion rules to identify live-safe review candidates, while remaining non-executing and manual-approval-only.

### Candidate Review Desk
Human review workflow for candidate acceptance/rejection/flagging with review metadata and priorities.

### Demo Deploy Desk
Queue workflow after review (`DEMO_QUEUE`, `DEMO_ASSIGNED`, `DEMO_PAUSED`, etc.) without broker deployment.

### Portfolio AI
Computes risk-aware allocation weights across live-safe candidates with diversification and correlation-aware constraints.

### Capital Engine
Simulates capital state (`total`, `allocated`, `free`, `risk_usage`, `growth_rate`) for control and reporting.

### Reporting Engine
Builds concise operator-facing summaries across health, pipeline, capital, and warnings.

### Operator Console
Centralized control/status layer aggregating system health, pipeline counts, capital posture, portfolio snapshot, and risk flags.
