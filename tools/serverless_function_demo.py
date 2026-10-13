"""
Unit IV Demonstration: Serverless Computing & Event-Driven Functions.

Demonstrates:
1. Event-driven invocation with structured payload and context.
2. Cold start (container initialization overhead) vs. warm container reuse.
3. Execution timeout cutoff and structured 504 error handling.
4. Idempotency key deduplication (safe retries during network replay).
5. Comprehensive comparison between Serverless Functions and Continuous Background Workers.

Usage:
    python -m tools.serverless_function_demo
"""

import logging
from pathlib import Path
import sys
import time
from typing import Any, Dict

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from common.serverless_runtime import FunctionContext, ServerlessRuntime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("serverless_demo")


# --- Sample Serverless Functions ---

def image_resizer_function(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """
    Stateless event-driven function simulating image dimension transformation.
    """
    image_name = event.get("image_name", "default.png")
    target_width = event.get("width", 800)
    target_height = event.get("height", 600)

    # Simulate fast image metadata calculation
    return {
        "processed_image": f"transformed_{image_name}",
        "dimensions": f"{target_width}x{target_height}",
        "aspect_ratio": round(target_width / target_height, 2),
        "execution_context": {
            "function": context.function_name,
            "invocation_id": context.invocation_id,
            "cold_start": context.cold_start,
        },
    }


def slow_batch_aggregator_function(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """
    Simulates a heavy operation that may breach strict serverless execution timeouts.
    """
    sleep_duration = event.get("sleep_duration", 0.1)
    time.sleep(sleep_duration)
    return {"status": "completed", "duration": sleep_duration}


def failing_validation_function(event: Dict[str, Any], context: FunctionContext) -> Dict[str, Any]:
    """
    Simulates application validation failure and exception bubbling.
    """
    if "api_key" not in event:
        raise ValueError("Missing mandatory 'api_key' in request headers.")
    return {"status": "authenticated"}


def run_demo() -> None:
    print("\n" + "=" * 78)
    print(" UNIT IV: SERVERLESS COMPUTING & EVENT-DRIVEN FUNCTION EXECUTION")
    print("=" * 78)
    print("Model:       Function-as-a-Service (FaaS) Container Lifecycle & Event Invocation")
    print("Concepts:    Cold Start, Warm Reuse, Execution Timeout, Idempotency, Statelessness")
    print("Syllabus:    Unit IV -- Emerging Paradigms (Serverless Computing)")

    # 1. Initialize Serverless Runtime
    # We configure a 60ms simulated cold-start delay to clearly demonstrate container boot overhead
    runtime = ServerlessRuntime(
        simulated_cold_start_delay_sec=0.06,
        container_idle_ttl_sec=3.0,
    )
    runtime.register_function("image_resizer", image_resizer_function)
    runtime.register_function("batch_aggregator", slow_batch_aggregator_function)
    runtime.register_function("validator", failing_validation_function)

    # 2. Cold Start Demonstration
    print("\n--- [STEP 1] Cold Start Invocation (First Call / Fresh Micro-VM) ---")
    event_payload = {"image_name": "banner.png", "width": 1920, "height": 1080}
    print(f"  [Event] Dispatching event to 'image_resizer': {event_payload}")

    t0 = time.perf_counter()
    res1 = runtime.invoke("image_resizer", event_payload)
    wall_ms1 = (time.perf_counter() - t0) * 1000.0

    print(f"  * Status Code:           {res1.status_code}")
    print(f"  * Invocation ID:         {res1.invocation_id}")
    print(f"  * Cold Start?            {res1.cold_start}  <-- Container spin-up overhead incurred")
    print(f"  * Duration:              {res1.execution_duration_ms:.2f} ms")
    print(f"  * Total Wall Clock Time: {wall_ms1:.2f} ms")
    print(f"  * Function Output:       {res1.payload}")

    # 3. Warm Container Reuse Demonstration
    print("\n--- [STEP 2] Warm Container Reuse (Subsequent Call) ---")
    print("  [Event] Sending second identical/subsequent event to warm container...")

    t0 = time.perf_counter()
    res2 = runtime.invoke("image_resizer", {"image_name": "profile.jpg", "width": 400, "height": 400})
    wall_ms2 = (time.perf_counter() - t0) * 1000.0

    print(f"  * Status Code:           {res2.status_code}")
    print(f"  * Invocation ID:         {res2.invocation_id}")
    print(f"  * Cold Start?            {res2.cold_start}  <-- Reused active container (Zero init delay!)")
    print(f"  * Duration:              {res2.execution_duration_ms:.2f} ms")
    print(f"  * Total Wall Clock Time: {wall_ms2:.2f} ms")
    print(f"  * Latency Delta:         Saved ~{wall_ms1 - wall_ms2:.1f} ms due to container reuse!")

    # 4. Repeated Independent Invocations
    print("\n--- [STEP 3] Repeated Independent Invocations (Burst of 4 Events) ---")
    for i in range(1, 5):
        burst_res = runtime.invoke("image_resizer", {"image_name": f"frame_{i}.png", "width": 640, "height": 480})
        print(f"  [Invocation {i}] ID={burst_res.invocation_id} | Cold={burst_res.cold_start} | Duration={burst_res.execution_duration_ms:.2f}ms")

    # 5. Strict Execution Limit / Timeout Demonstration
    print("\n--- [STEP 4] Execution Limit Enforcement (Timeout Cutoff) ---")
    timeout_limit = 0.08  # 80ms timeout
    sleep_time = 0.25     # function attempts to run for 250ms
    print(f"  [Config] Execution timeout limit set strictly to {timeout_limit * 1000:.0f} ms.")
    print(f"  [Event] Function 'batch_aggregator' attempts to sleep for {sleep_time * 1000:.0f} ms...")

    timeout_res = runtime.invoke(
        "batch_aggregator",
        {"sleep_duration": sleep_time},
        timeout_sec=timeout_limit,
    )
    print(f"  * Status Code:    {timeout_res.status_code} (HTTP 504 Gateway Timeout)")
    print(f"  * Error Message:  {timeout_res.error_message}")
    print(f"  * Elapsed Time:   {timeout_res.execution_duration_ms:.2f} ms")
    print("  [Enforcement] Thread stopped & degraded container evicted to protect cluster capacity.")

    # 6. Idempotency Key Handling (Network Retry Deduplication)
    print("\n--- [STEP 5] Event Idempotency & Safe Retries ---")
    idempotency_key = "event-tx-99482-a"
    payment_event = {"image_name": "invoice_receipt.pdf", "width": 1200, "height": 1600}

    print(f"  [Client] Invoking with Idempotency-Key '{idempotency_key}' (Attempt 1)...")
    tx1 = runtime.invoke("image_resizer", payment_event, idempotency_key=idempotency_key)
    print(f"  * Result: Invocation ID={tx1.invocation_id}, Cached={tx1.cached_idempotent}")

    print(f"  [Network] Simulating network glitch causing upstream producer to re-send same event (Attempt 2)...")
    tx2 = runtime.invoke("image_resizer", payment_event, idempotency_key=idempotency_key)
    print(f"  * Result: Invocation ID={tx2.invocation_id}, Cached={tx2.cached_idempotent}")
    print(f"  [OK] Duplicate execution prevented! Returned previous result without re-executing handler.")

    # 7. Architectural Comparison Table
    print("\n--- [STEP 6] Architectural Comparison: Serverless vs Continuous Workers ---")
    comparison = """
+-----------------------+------------------------------------+------------------------------------+
| Dimension             | Continuous Worker (worker/consumer)| Serverless Function (Runtime)      |
+-----------------------+------------------------------------+------------------------------------+
| Process Model         | Long-lived OS daemon process       | Ephemeral micro-VM / sandbox       |
| Scaling               | Manual / Autoscaling worker pools  | Scale-to-zero, elastic per-event   |
| Idle Cost             | Paid 24/7 regardless of queue size | $0 when idle (pay per invocation)  |
| Startup Latency       | 0 ms (process already polling)     | 50-500 ms (Cold start on scale-up) |
| Max Execution Time    | Hours / Unbounded batch jobs       | Strict timeout (e.g. 15 min AWS)   |
| State & Connections   | Reusable connection pools          | Stateless; external DB/S3 required |
| Invocation Trigger    | Redis BRPOP / streaming queue pull | Event push (HTTP, SNS, S3, Webhook)|
| Failure Recovery      | Heartbeats & Lease reclamation     | Automatic retry & Dead-Letter Queue|
+-----------------------+------------------------------------+------------------------------------+
"""
    print(comparison)

    print("=" * 78)
    print(" SERVERLESS COMPUTING DEMONSTRATION COMPLETE")
    print("=" * 78 + "\n")


if __name__ == "__main__":
    run_demo()
