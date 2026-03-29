# Algo Sphere Metrics Guide

## Strategy Quality Metrics

### `fitness_score`
Composite quality score used for ranking strategy candidates in factory/evolution flows.

### `promotion_score`
Feedback-driven score indicating progression readiness after paper performance review.

### `paper_win_rate`
Win ratio observed in paper simulation; used in feedback and promotion logic.

### `paper_drawdown`
Drawdown observed in paper mode; key risk control metric in progression checks.

### `risk_profile`
Categorical risk classification (`LOW`, `MEDIUM`, `HIGH`) used by safe-mode and queue prioritization.

## Allocation and Capital Metrics

### `weight` / allocation weight
Relative portfolio share assigned by Portfolio AI, converted into capital percent.

### `capital_percent`
Portfolio-level percentage assigned to a strategy (bounded by policy constraints).

### `total_capital`, `allocated`, `free`
Capital Engine simulation snapshot of available and committed capital.

### `risk_usage`
Fraction of capital currently considered allocated in simulation.

### `growth_rate`
Simulated growth indicator based on portfolio/fund performance snapshots.

## Operational Metrics
- Candidate counts by lifecycle/review/demo state.
- Paper running/success counts.
- Live-safe readiness counts.
- Auto-loop cycle counts and health signals.
