import argparse
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml


@dataclass
class RunnerConfig:
    api_url: str = "http://127.0.0.1:8000"
    poll_interval: int = 5
    runner_id: str = "demo_runner_alpha"
    max_parallel_jobs: int = 2
    jobs_limit: int = 5


def load_config(path: Path) -> RunnerConfig:
    if not path.exists():
        return RunnerConfig()
    with path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    return RunnerConfig(
        api_url=str(payload.get("api_url", "http://127.0.0.1:8000")),
        poll_interval=int(payload.get("poll_interval", 5)),
        runner_id=str(payload.get("runner_id", "demo_runner_alpha")),
        max_parallel_jobs=int(payload.get("max_parallel_jobs", 2)),
        jobs_limit=int(payload.get("jobs_limit", 5)),
    )


def _log(message: str) -> None:
    print(f"[RUNNER] {message}")


def _post(api_url: str, path: str, params: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
    response = requests.post(f"{api_url}{path}", params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def process_job(job: dict[str, Any], config: RunnerConfig) -> None:
    strategy_id = str(job.get("strategy_id", ""))
    if not strategy_id:
        return
    short_id = strategy_id[:8]
    try:
        _log(f"Found job {short_id}")
        _log("ACK")
        _post(
            config.api_url,
            "/runner/ack",
            {
                "strategy_id": strategy_id,
                "runner_id": config.runner_id,
                "note": "Acknowledged by first demo runner.",
            },
        )

        _log("START")
        _post(
            config.api_url,
            "/runner/start",
            {"strategy_id": strategy_id, "note": "Started by first demo runner."},
        )

        _log("Simulating...")
        duration = random.randint(5, 20)
        time.sleep(duration)

        success = random.random() < 0.8
        if success:
            _log("COMPLETE")
            _post(
                config.api_url,
                "/runner/complete",
                {
                    "strategy_id": strategy_id,
                    "note": f"Simulation completed successfully in {duration}s.",
                },
            )
        else:
            _log("FAIL")
            _post(
                config.api_url,
                "/runner/fail",
                {
                    "strategy_id": strategy_id,
                    "note": f"Simulation failed after {duration}s (safe random failure).",
                },
            )
    except requests.RequestException as exc:
        _log(f"HTTP error on {short_id}: {exc}")
        try:
            _post(
                config.api_url,
                "/runner/fail",
                {
                    "strategy_id": strategy_id,
                    "note": f"Runner exception: {exc}",
                },
            )
        except requests.RequestException:
            _log(f"Could not mark fail for {short_id}")
    except Exception as exc:  # broad safe guard for loop continuity
        _log(f"Unhandled error on {short_id}: {exc}")


def fetch_jobs(config: RunnerConfig) -> list[dict[str, Any]]:
    response = requests.get(
        f"{config.api_url}/runner/jobs",
        params={"limit": config.jobs_limit},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    jobs = payload.get("jobs", [])
    return [job for job in jobs if job.get("eligible") is True]


def run_loop(config: RunnerConfig) -> None:
    _log(f"Starting runner_id={config.runner_id} api_url={config.api_url}")
    _log("Safety mode: demo only, no broker execution, no real trading.")
    in_flight: set[str] = set()
    workers: list[threading.Thread] = []

    while True:
        workers = [w for w in workers if w.is_alive()]
        try:
            jobs = fetch_jobs(config)
            capacity = max(0, config.max_parallel_jobs - len(workers))
            if jobs and capacity > 0:
                for job in jobs[:capacity]:
                    sid = str(job.get("strategy_id", ""))
                    if not sid or sid in in_flight:
                        continue
                    in_flight.add(sid)

                    def _worker(selected_job: dict[str, Any], selected_id: str) -> None:
                        try:
                            process_job(selected_job, config)
                        finally:
                            in_flight.discard(selected_id)

                    t = threading.Thread(
                        target=_worker,
                        args=(job, sid),
                        daemon=True,
                    )
                    workers.append(t)
                    t.start()
        except requests.RequestException as exc:
            _log(f"Polling error: {exc}")
        except Exception as exc:  # broad safe guard for loop continuity
            _log(f"Loop error: {exc}")

        time.sleep(config.poll_interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Algo Sphere First Demo Runner")
    parser.add_argument(
        "--config",
        default="runner/config.yaml",
        help="Path to YAML config file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config))
    run_loop(config)


if __name__ == "__main__":
    main()
