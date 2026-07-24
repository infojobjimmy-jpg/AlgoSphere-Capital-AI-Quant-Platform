"""Execution adapters (paper / live) separate from signal generation."""

from app.execution.executor import execute_trade

__all__ = ["execute_trade"]
