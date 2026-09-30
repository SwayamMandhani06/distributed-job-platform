"""
Load Generator for Distributed Job Platform.

Simulates a continuous stream of incoming jobs for live demonstrations,
stress-testing, and dynamic dashboard visualization.

Usage:
    # Submit 30 jobs at 0.5s interval (default)
    python tests/load_generator.py

    # Submit 50 jobs at 0.2s interval
    python tests/load_generator.py --count 50 --interval 0.2

    # Run continuously for 60 seconds (duration mode overrides count)
    python tests/load_generator.py --duration 60 --interval 0.5
"""

import argparse
import random
import time
from typing import Any, Dict

import requests

DEFAULT_SCHEDULER_URL = "http://localhost:8000"

# Sample job templates matching payload shapes used in tests/test_client.py
SAMPLE_JOB_TEMPLATES = [
    lambda: {
        "job_type": "email",
        "payload": {
            "to": random.choice([
                "user@example.com",
                "alice@example.com",
                "bob@example.com",
                "carol@example.com",
                "david@example.com",
            ]),
            "subject": random.choice([
                "Welcome!",
                "Invoice #4029",
                "Password Reset",
                "Weekly Digest",
                "Security Alert",
            ]),
            "body": "Thanks for signing up.",
        },
    },
    lambda: {
        "job_type": "csv_processing",
        "payload": {
            "file_path": f"/data/uploads/sales_q{random.randint(1, 4)}.csv",
            "operations": random.choice([
                ["deduplicate", "aggregate"],
                ["filter", "sort"],
                ["aggregate", "export"],
            ]),
        },
    },
    lambda: {
        "job_type": "thumbnail",
        "payload": {
            "image_url": f"https://example.com/photos/{random.choice(['landscape', 'avatar', 'banner', 'product'])}_{random.randint(1, 50)}.jpg",
            "sizes": [128, 256, 512],
        },
    },
]


def generate_job() -> Dict[str, Any]:
    """Return a randomly chosen job payload matching sample shapes."""
    return random.choice(SAMPLE_JOB_TEMPLATES)()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Submit a continuous stream of jobs to the Distributed Job Platform for demo purposes."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=30,
        help="Total number of jobs to submit (default: 30). Ignored if --duration is set.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Seconds to wait between each submission (default: 0.5).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Continuous run duration in seconds (overrides --count for live background demo).",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=DEFAULT_SCHEDULER_URL,
        help=f"Base URL of the scheduler service (default: {DEFAULT_SCHEDULER_URL}).",
    )

    args = parser.parse_args()

    base_url = args.url.rstrip("/")
    submit_url = f"{base_url}/submit_job"
    dashboard_url = f"{base_url}/"

    print("=" * 70)
    print("  Distributed Job Platform -- Demo Load Generator")
    print("=" * 70)
    if args.duration is not None:
        print(f"  Mode     : Continuous Duration ({args.duration}s)")
        print(f"  Interval : {args.interval}s")
    else:
        print(f"  Mode     : Fixed Count ({args.count} jobs)")
        print(f"  Interval : {args.interval}s")
    print(f"  Endpoint : {submit_url}")
    print(f"  Dashboard: {dashboard_url}")
    print("-" * 70)

    job_counter = 0
    submitted_count = 0
    start_time = time.time()

    try:
        while True:
            # Check stopping condition
            if args.duration is not None:
                elapsed = time.time() - start_time
                if elapsed >= args.duration:
                    break
            else:
                if job_counter >= args.count:
                    break

            job_counter += 1
            job_data = generate_job()
            job_type = job_data["job_type"]

            try:
                resp = requests.post(submit_url, json=job_data, timeout=5)
                resp.raise_for_status()
                data = resp.json()

                job_id = data.get("job_id", "N/A")
                short_id = job_id[:8] if len(job_id) >= 8 else job_id
                status = data.get("status", "accepted")

                # Single-line confirmation: job number, type, job_id, short
                print(
                    f"[{job_counter:>3}] Type: {job_type:<14} | "
                    f"Job ID: {job_id} | Short: {short_id} | Status: {status}"
                )
                submitted_count += 1

            except requests.exceptions.ConnectionError:
                print(
                    f"[{job_counter:>3}] Type: {job_type:<14} | "
                    f"FAILED: Connection refused (is scheduler running at {submit_url}?)"
                )
            except requests.exceptions.RequestException as exc:
                print(
                    f"[{job_counter:>3}] Type: {job_type:<14} | "
                    f"FAILED: {exc}"
                )

            # Sleep between submissions
            if args.interval > 0:
                if args.duration is not None:
                    remaining_time = args.duration - (time.time() - start_time)
                    if remaining_time <= 0:
                        break
                    time.sleep(min(args.interval, remaining_time))
                else:
                    if job_counter < args.count:
                        time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\n[Interrupted] Stopped load generator (Ctrl+C).")

    print("\n" + "=" * 70)
    print("  Load Generation Finished")
    print(f"  Total jobs submitted : {submitted_count}")
    print(f"  Live dashboard       : {dashboard_url}")
    print("=" * 70)


if __name__ == "__main__":
    main()
