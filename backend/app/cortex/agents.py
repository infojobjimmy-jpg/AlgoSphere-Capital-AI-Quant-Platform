"""
Logical agent roles in the unified architecture.
Each role maps to existing modules — this file is the roster only (no duplicate logic).
"""

from __future__ import annotations

# Ordered stages for meta.cortex and observability (names match the architecture spec).
PIPELINE: tuple[tuple[str, str], ...] = (
    ("observer", "app.fusion.event_engine + app.agents.watcher"),
    ("analyst", "app.agents.anomaly_agent + app.agents.anomaly_v2"),
    ("investigator", "app.agents.investigator"),
    ("narrator", "app.agents.narrator"),
    ("strategist", "app.agents.strategic_agent"),
    ("planner", "app.goals.planning_agent"),
    ("executor", "app.goals.execution_engine"),
    ("critic", "app.cortex.critic"),
    ("memory", "app.memory.chroma_memory + Timescale persistence"),
    ("evolution", "app.meta_learning.evolution_agent (offline)"),
    ("evolutionary", "app.meta_learning.evolutionary.round (offline)"),
)
