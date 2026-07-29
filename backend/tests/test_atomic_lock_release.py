"""
Tests for the atomic Redis lock release in whop_fulfillment.release_lock().

Covers:
  1. Token matches → lock deleted (eval returns 1)
  2. Token differs → lock intact (eval returns 0)
  3. Lock expired and re-acquired with new token → old worker does not
     delete the new lock
  4. Redis error during release → error swallowed, no token logged,
     main result preserved
  5. Two concurrent fulfill_membership() calls → exactly one email sent

Run:
    cd backend && python -m pytest tests/test_atomic_lock_release.py -v
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from app.services.whop_fulfillment import release_lock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LOCK_KEY = "acap:fulfillment:lock:mem_test_lock"
_TOKEN_A = "aaaa1111aaaa1111aaaa1111aaaa1111"
_TOKEN_B = "bbbb2222bbbb2222bbbb2222bbbb2222"

_BASE_MEMBERSHIP = {
    "id": "mem_lock_test",
    "status": "active",
    "license_key": "LK-LOCK-001",
    "manage_url": "https://whop.com/hub/test",
    "product": {"id": "prod_bHg2Q9qH34ABM", "title": "AlgoSphere Test"},
    "user": {"email": "lock-test@example.com"},
}


# ---------------------------------------------------------------------------
# 1. Token matches → lock deleted
# ---------------------------------------------------------------------------

async def test_release_lock_matching_token_deletes_key():
    redis_mock = AsyncMock()
    redis_mock.eval = AsyncMock(return_value=1)

    await release_lock(redis_mock, _LOCK_KEY, _TOKEN_A)

    redis_mock.eval.assert_awaited_once()
    call_args = redis_mock.eval.call_args
    # KEYS[1] and ARGV[1] are the positional args after the script and numkeys
    assert call_args[0][2] == _LOCK_KEY   # lock_key
    assert call_args[0][3] == _TOKEN_A    # lock_token


# ---------------------------------------------------------------------------
# 2. Token differs → lock intact (eval returns 0, no separate delete called)
# ---------------------------------------------------------------------------

async def test_release_lock_different_token_does_not_delete():
    redis_mock = AsyncMock()
    redis_mock.eval = AsyncMock(return_value=0)

    await release_lock(redis_mock, _LOCK_KEY, _TOKEN_B)

    redis_mock.eval.assert_awaited_once()
    # Confirm no separate delete was issued
    redis_mock.delete.assert_not_awaited()


# ---------------------------------------------------------------------------
# 3. Expired-and-re-acquired: old token no longer matches new token
#    → old worker's release_lock call must return 0 and leave lock intact
# ---------------------------------------------------------------------------

async def test_release_lock_expired_and_reacquired_by_other_worker():
    """
    Scenario:
      - Worker A holds lock with TOKEN_A (ex=300).
      - Lock expires; Worker B acquires it with TOKEN_B.
      - Worker A finishes and calls release_lock(..., TOKEN_A).
      - Lua: get(key) == TOKEN_B != TOKEN_A → returns 0 (no delete).
      - Worker B's lock survives.
    """
    # Simulate the Redis state after re-acquisition: key holds TOKEN_B
    redis_mock = AsyncMock()
    # Lua sees TOKEN_B stored but receives TOKEN_A → returns 0
    redis_mock.eval = AsyncMock(return_value=0)

    result = await release_lock(redis_mock, _LOCK_KEY, _TOKEN_A)

    redis_mock.eval.assert_awaited_once()
    # Verify the token passed to eval is TOKEN_A (the stale worker's token)
    call_args = redis_mock.eval.call_args
    assert call_args[0][3] == _TOKEN_A
    # eval returned 0 → no deletion occurred, Worker B's lock is safe
    redis_mock.delete.assert_not_awaited()


# ---------------------------------------------------------------------------
# 4. Redis error during release → swallowed, no token logged, result preserved
# ---------------------------------------------------------------------------

async def test_release_lock_redis_error_is_swallowed(caplog):
    redis_mock = AsyncMock()
    redis_mock.eval = AsyncMock(side_effect=ConnectionError("Redis unavailable"))

    with caplog.at_level(logging.WARNING, logger="acap.fulfillment"):
        # Must not raise
        await release_lock(redis_mock, _LOCK_KEY, _TOKEN_A)

    # A warning is logged but the token is never in it
    warning_text = " ".join(caplog.messages)
    assert _TOKEN_A not in warning_text
    assert _TOKEN_B not in warning_text
    # Lock key (not sensitive) may appear; token must not
    for record in caplog.records:
        assert _TOKEN_A not in record.getMessage()


async def test_release_lock_error_does_not_mask_main_result():
    """fulfill_membership() returns the correct (sent, reason) even when
    the lock release raises.  The error is swallowed in the finally block."""
    redis_mock = AsyncMock()
    # Lock acquired (SET NX → True)
    redis_mock.set = AsyncMock(return_value=True)
    # Lock release raises
    redis_mock.eval = AsyncMock(side_effect=RuntimeError("Redis blip"))
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.aclose = AsyncMock()

    with (
        patch("app.config.settings.app_slug", "acap"),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.services.whop_fulfillment.is_fulfilled", AsyncMock(return_value=False)),
        patch("app.services.whop_fulfillment.send_license_email", AsyncMock()),
        patch("app.services.whop_fulfillment.record_fulfillment", AsyncMock()),
    ):
        from app.services.whop_fulfillment import fulfill_membership
        sent, reason = await fulfill_membership(
            "mem_lock_test", _BASE_MEMBERSHIP, "api_key", redis_mock, "test"
        )

    # The main result must be preserved despite the release error
    assert sent is True
    assert reason == "sent"


# ---------------------------------------------------------------------------
# 5. Two concurrent fulfill calls → exactly one email sent
# ---------------------------------------------------------------------------

async def test_concurrent_two_workers_only_one_email():
    """
    Worker A acquires the lock; Worker B's SET NX returns None (lock held).
    Only Worker A sends the email.
    """
    import secrets as _secrets

    email_mock = AsyncMock()
    record_mock = AsyncMock()
    is_fulfilled_mock = AsyncMock(return_value=False)

    # Redis mock: first SET NX succeeds, second fails (returns None)
    set_call_count = 0

    async def mock_set(key, value, nx=False, ex=None):
        nonlocal set_call_count
        set_call_count += 1
        return True if set_call_count == 1 else None

    redis_mock = AsyncMock()
    redis_mock.set = AsyncMock(side_effect=mock_set)
    redis_mock.eval = AsyncMock(return_value=1)
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.aclose = AsyncMock()

    with (
        patch("app.config.settings.app_slug", "acap"),
        patch("app.config.settings.whop_allowed_product_ids", "prod_bHg2Q9qH34ABM"),
        patch("app.services.whop_fulfillment.is_fulfilled", is_fulfilled_mock),
        patch("app.services.whop_fulfillment.send_license_email", email_mock),
        patch("app.services.whop_fulfillment.record_fulfillment", record_mock),
    ):
        from app.services.whop_fulfillment import fulfill_membership

        result_a, result_b = await asyncio.gather(
            fulfill_membership("mem_lock_test", _BASE_MEMBERSHIP, "key", redis_mock, "worker_a"),
            fulfill_membership("mem_lock_test", _BASE_MEMBERSHIP, "key", redis_mock, "worker_b"),
        )

    # Exactly one email sent
    assert email_mock.await_count == 1
    # One worker sent, the other was locked out
    results = {result_a[1], result_b[1]}
    assert "sent" in results
    assert "lock_held" in results
