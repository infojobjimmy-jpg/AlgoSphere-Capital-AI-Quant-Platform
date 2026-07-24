"""Unified cognitive loop: goals → perception → agents → plan → execute → critic → memory."""

from app.cortex.cycle import run_kafka_intel_tick

__all__ = ["run_kafka_intel_tick"]
