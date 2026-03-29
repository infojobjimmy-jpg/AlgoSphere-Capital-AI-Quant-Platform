# Algo Sphere Safety Model

## Core Safety Principles
- **No live trading** from the API stack.
- **No broker execution** in backend workflows.
- **Human approval required** before demo progression steps.
- **Capital is simulated** for monitoring and decision support.
- **Demo workflow is queue-only**, not deployment.

## Safety Controls by Layer
- **Paper Trading**: simulation only, fixed constraints, no execution adapter.
- **Live Safe Mode**: classification and gating only.
- **Candidate Review Desk**: explicit human decision metadata.
- **Demo Deploy Desk**: status and assignment workflow only.
- **Operator Console**: read-only aggregation and risk signaling.

## Risk Flags and Monitoring
Algo Sphere surfaces warnings such as:
- High capital risk usage.
- Full capital allocation pressure.
- Low/no paper success conditions.
- Candidate volume overload.

These flags are monitoring outputs and do not trigger autonomous market execution.

## What Is Explicitly Not Enabled
- Auto-trading
- Broker order placement
- Real capital deployment
- Unsupervised transition from research state to execution
