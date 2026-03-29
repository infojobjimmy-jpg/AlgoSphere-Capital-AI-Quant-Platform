# Algo Sphere Investor Pack

## Executive Summary
Algo Sphere is an autonomous multi-strategy research and control platform designed to generate, evaluate, and prioritize algorithmic strategies in a safety-first environment. It combines AI-driven strategy discovery with strict human oversight and non-executing workflow gates.

## Problem
- Traditional trading systems are fragile when a single model degrades.
- Manual trading is inconsistent, hard to scale, and decision-latency sensitive.
- Single-strategy exposure increases concentration and regime risk.

## Solution
Algo Sphere provides an autonomous multi-strategy engine with:
- Continuous candidate generation and evolution.
- Simulation-first validation loops.
- Structured human review and demo queue workflows.
- Portfolio-level allocation logic and operator monitoring.

## Technology
- **Bot Factory + Evolution Engine**: create and improve candidates.
- **Paper Trading + Feedback**: validate quality and derive promotion signals.
- **Live Safe Mode**: enforce safety gating before progression.
- **Candidate Review Desk**: human approval and prioritization.
- **Demo Deploy Desk**: controlled queue and assignment workflow.
- **Portfolio AI + Capital Engine**: simulated allocation and risk posture.
- **Reporting + Meta AI + Operator Console**: centralized observability.

## Safety
- No live trading execution in API.
- No broker execution path in workflow modules.
- Human-in-the-loop approvals for critical progress transitions.
- Capital is simulation-based for decision support.
- Risk flags surface pressure conditions early.

## Current Status (Snapshot)
Based on latest local operator console snapshot:
- Strategies generated: **757**
- Paper success: **5**
- Live safe ready: **4**
- Demo queued: **1**
- System health: **WARNING**
- Risk mode: **NORMAL**

## Vision
- Expand to multi-market strategy research (FX, indices, commodities, crypto).
- Evolve into a fund-grade autonomous research and allocation engine.
- Enable a controlled strategy marketplace with explicit governance and review.
