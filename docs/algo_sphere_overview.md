# Algo Sphere Overview

## Overview
Algo Sphere is a local-first autonomous strategy research platform built to discover, evolve, and evaluate trading strategies in a controlled environment. It combines modular AI/quant components with strict safety gates and operator review workflows.

## Goals
- Continuously generate and improve strategy candidates.
- Validate candidates through simulated paper performance.
- Keep humans in control before any progression to demo workflows.
- Provide transparent monitoring through unified dashboards and API endpoints.

## System Capabilities
- Bot and strategy lifecycle management.
- Strategy generation and mutation/evolution.
- Paper-trading simulation and feedback scoring.
- Live-safe qualification and review workflows.
- Demo deploy queue management (queue/assign/pause/reject only).
- Portfolio allocation and capital simulation.
- Meta-level monitoring, reporting, and operator console aggregation.

## Design Philosophy
- Safety-first: no live trading and no broker execution from the API.
- Explainable modules: each layer has focused responsibilities.
- Incremental architecture: add capabilities without breaking previous phases.
- Human-in-the-loop: review desks and operator console remain decision authorities.
- Local production readiness: simple stack, strong observability, clear contracts.
