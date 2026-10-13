"""
Unit tests for Unit III Synchronization primitives:
1. Lamport Logical Clocks (Leslie Lamport, 1978)
2. Vector Clocks (Fidge / Mattern, 1988)
3. Cristian's Physical Clock Synchronization Algorithm (Flaviu Cristian, 1989)
"""

import threading
import pytest
from unittest.mock import MagicMock, patch

from common.clocks import LamportClock, VectorClock
from common.cristian import compute_cristian_sync, query_cristian_server


class TestLamportClock:
    """Tests for Lamport scalar logical clock rules and causal ordering."""

    def test_initial_value(self):
        clock_zero = LamportClock()
        assert clock_zero.get_time() == 0

        clock_custom = LamportClock(initial_time=42)
        assert clock_custom.get_time() == 42

    def test_rule_1_local_event_increments_counter(self):
        """Rule 1: L = L + 1 on local event."""
        clock = LamportClock(initial_time=0)
        t1 = clock.tick()
        assert t1 == 1
        assert clock.get_time() == 1

        t2 = clock.tick()
        assert t2 == 2
        assert clock.get_time() == 2

    def test_rule_1_send_event_increments_counter(self):
        """Rule 1: L = L + 1 before sending message."""
        clock = LamportClock(initial_time=5)
        sent_ts = clock.send_event()
        assert sent_ts == 6
        assert clock.get_time() == 6

    def test_rule_2_receive_event_advances_clock(self):
        """Rule 2: L = max(L, T_msg) + 1 on receiving message timestamp T_msg."""
        # Case A: Incoming timestamp is greater than local clock
        clock_a = LamportClock(initial_time=3)
        updated_a = clock_a.receive_event(received_time=10)
        assert updated_a == 11
        assert clock_a.get_time() == 11

        # Case B: Local clock is greater than incoming timestamp
        clock_b = LamportClock(initial_time=20)
        updated_b = clock_b.receive_event(received_time=8)
        assert updated_b == 21
        assert clock_b.get_time() == 21

        # Case C: Incoming timestamp equals local clock
        clock_c = LamportClock(initial_time=15)
        updated_c = clock_c.receive_event(received_time=15)
        assert updated_c == 16
        assert clock_c.get_time() == 16

    def test_causal_ordering_between_processes(self):
        """Verify that e1 -> e2 implies L(e1) < L(e2)."""
        proc_a = LamportClock()
        proc_b = LamportClock()

        # Proc A performs local event eA1
        e_a1 = proc_a.tick()  # 1

        # Proc A sends message to Proc B (eA2)
        msg_ts = proc_a.send_event()  # 2

        # Proc B receives message from Proc A (eB1)
        e_b1 = proc_b.receive_event(msg_ts)  # max(0, 2) + 1 = 3

        # Proc B performs local event eB2
        e_b2 = proc_b.tick()  # 4

        # Causal property holds: eA1 -> eA2 -> eB1 -> eB2
        assert e_a1 < msg_ts
        assert msg_ts < e_b1
        assert e_b1 < e_b2

    def test_thread_safety_under_concurrent_ticks(self):
        """Verify atomic increments across concurrent threads."""
        clock = LamportClock(initial_time=0)
        num_threads = 10
        ticks_per_thread = 500

        def worker():
            for _ in range(ticks_per_thread):
                clock.tick()

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert clock.get_time() == num_threads * ticks_per_thread


class TestVectorClock:
    """Tests for Vector Clock rules, comparison, and concurrency detection."""

    def test_initialization(self):
        vc = VectorClock("P1", ["P1", "P2", "P3"])
        clock = vc.get_clock()
        assert clock == {"P1": 0, "P2": 0, "P3": 0}

    def test_rule_1_local_event(self):
        vc = VectorClock("P1", ["P1", "P2"])
        state = vc.local_event()
        assert state["P1"] == 1
        assert state["P2"] == 0

    def test_rule_2_send_event(self):
        vc = VectorClock("P2", ["P1", "P2"])
        state = vc.send_event()
        assert state["P2"] == 1
        assert state["P1"] == 0

    def test_rule_3_receive_event_component_max_and_increment(self):
        vc = VectorClock("P2", ["P1", "P2", "P3"])
        # P2 has had one local event: {"P1": 0, "P2": 1, "P3": 0}
        vc.local_event()

        # P2 receives vector from P1: {"P1": 3, "P2": 0, "P3": 2}
        incoming = {"P1": 3, "P2": 0, "P3": 2}
        updated = vc.receive_event(incoming)

        # Expected:
        # P1 = max(0, 3) = 3
        # P2 = max(1, 0) + 1 = 2
        # P3 = max(0, 2) = 2
        assert updated == {"P1": 3, "P2": 2, "P3": 2}

    def test_vector_comparison_equal(self):
        v1 = {"P1": 2, "P2": 1, "P3": 0}
        v2 = {"P1": 2, "P2": 1, "P3": 0}
        assert VectorClock.compare(v1, v2) == "EQUAL"

    def test_vector_comparison_before_after(self):
        # v1 strictly causally precedes v2: v1 -> v2
        v1 = {"P1": 1, "P2": 0, "P3": 0}
        v2 = {"P1": 2, "P2": 1, "P3": 0}
        assert VectorClock.compare(v1, v2) == "BEFORE"
        assert VectorClock.compare(v2, v1) == "AFTER"

    def test_vector_comparison_concurrent(self):
        # Independent events on P1 and P2: neither precedes the other
        v_p1 = {"P1": 1, "P2": 0, "P3": 0}
        v_p2 = {"P1": 0, "P2": 1, "P3": 0}
        assert VectorClock.compare(v_p1, v_p2) == "CONCURRENT"

        # Another concurrency case:
        v_a = {"P1": 2, "P2": 1, "P3": 0}
        v_b = {"P1": 1, "P2": 3, "P3": 0}
        assert VectorClock.compare(v_a, v_b) == "CONCURRENT"

    def test_three_process_causal_and_concurrent_scenario(self):
        """Demonstrate 3 processes where e1 || e2 and e1 -> e4."""
        p1 = VectorClock("P1", ["P1", "P2", "P3"])
        p2 = VectorClock("P2", ["P1", "P2", "P3"])
        p3 = VectorClock("P3", ["P1", "P2", "P3"])

        # e1: P1 local event
        e1 = p1.local_event()  # P1: [1, 0, 0]

        # e2: P2 local event (independent of P1)
        e2 = p2.local_event()  # P2: [0, 1, 0]

        # Verify e1 and e2 are concurrent
        assert VectorClock.compare(e1, e2) == "CONCURRENT"

        # e3: P1 sends to P3
        msg_e3 = p1.send_event()  # P1: [2, 0, 0]

        # e4: P3 receives message from P1
        e4 = p3.receive_event(msg_e3)  # P3: [2, 0, 1]

        # Verify causal relationship: e1 happened before e4
        assert VectorClock.compare(e1, e4) == "BEFORE"
        assert VectorClock.compare(e4, e1) == "AFTER"

        # Verify that e2 (on P2) is still concurrent with e4 (on P3)
        assert VectorClock.compare(e2, e4) == "CONCURRENT"


class TestCristianClockSynchronization:
    """Tests for Cristian's physical clock synchronization algorithm calculations."""

    def test_deterministic_calculation_symmetric_delay(self):
        """Deterministic test with known timestamps under symmetric delay assumption.

        Scenario:
          Client sends at T0 = 100.000
          Server receives & responds at T_server = 105.010
          Client receives response at T1 = 100.040
        Calculations:
          RTT = 100.040 - 100.000 = 0.040 (40ms)
          half_rtt = 0.020 (20ms)
          Estimated server time at T1 = 105.010 + 0.020 = 105.030
          Offset theta = 105.010 - ((100.000 + 100.040) / 2) = 105.010 - 100.020 = +4.990s
          Uncertainty = +/- 0.020s
        """
        result = compute_cristian_sync(t0=100.000, t_server=105.010, t1=100.040)

        assert pytest.approx(result["round_trip_delay_seconds"], abs=1e-5) == 0.040
        assert pytest.approx(result["estimated_server_time_at_t1"], abs=1e-5) == 105.030
        assert pytest.approx(result["clock_offset_seconds"], abs=1e-5) == 4.990
        assert pytest.approx(result["uncertainty_seconds"], abs=1e-5) == 0.020

    def test_client_ahead_of_server_negative_offset(self):
        """Scenario where client clock is ahead of server."""
        # Client T0 = 200.0, T1 = 200.06 (midpoint 200.03)
        # Server T_server = 195.00
        # Offset = 195.00 - 200.03 = -5.03s
        result = compute_cristian_sync(t0=200.0, t_server=195.0, t1=200.06)

        assert pytest.approx(result["round_trip_delay_seconds"], abs=1e-5) == 0.060
        assert pytest.approx(result["clock_offset_seconds"], abs=1e-5) == -5.030
        assert pytest.approx(result["uncertainty_seconds"], abs=1e-5) == 0.030

    def test_zero_latency_edge_case(self):
        """Zero RTT edge case."""
        result = compute_cristian_sync(t0=100.0, t_server=100.0, t1=100.0)
        assert result["round_trip_delay_seconds"] == 0.0
        assert result["clock_offset_seconds"] == 0.0
        assert result["uncertainty_seconds"] == 0.0

    def test_invalid_timestamps_raises_value_error(self):
        """Receive time before send time must be rejected."""
        with pytest.raises(ValueError, match="cannot be earlier than send time"):
            compute_cristian_sync(t0=100.0, t_server=100.0, t1=99.0)

    def test_query_cristian_server_mocked_http(self):
        """Test query_cristian_server against mocked HTTP response with injected time source."""
        fake_time_seq = [100.0, 100.05]
        fake_time_iter = iter(fake_time_seq)

        def mock_time():
            return next(fake_time_iter)

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"server_time": 105.025, "iso": "2026-10-12T00:00:00Z"}

        with patch("requests.get", return_value=mock_response) as mock_get:
            result = query_cristian_server(
                server_url="http://localhost:8000",
                time_source=mock_time,
            )
            mock_get.assert_called_once_with("http://localhost:8000/api/time", timeout=5.0)

        # T0=100.0, T1=100.05, midpoint=100.025, T_server=105.025
        # RTT = 0.05, offset = 105.025 - 100.025 = 5.0
        assert pytest.approx(result["round_trip_delay_seconds"], abs=1e-4) == 0.05
        assert pytest.approx(result["clock_offset_seconds"], abs=1e-4) == 5.0
        assert pytest.approx(result["uncertainty_seconds"], abs=1e-4) == 0.025
        assert result["actual_os_clock_adjusted"] is False
        assert result["simulated_clock_offset"] is True

    def test_injected_delay_and_jitter(self):
        """Test query_cristian_server with simulated network delay and jitter."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "server_time": 102.5,
            "server_receive_time": 102.49,
            "server_send_time": 102.50,
        }

        with patch("requests.get", return_value=mock_response):
            result = query_cristian_server(
                server_url="http://localhost:8000",
                injected_delay_seconds=0.04,
                injected_jitter_seconds=0.01,
            )
        assert result["round_trip_delay_seconds"] >= 0.04
        assert result["actual_os_clock_adjusted"] is False
        assert result["simulated_clock_offset"] is True

    def test_invalid_server_responses(self):
        """Test rejection of malformed or error server responses."""
        # Case 1: Missing server_time
        mock_bad_json = MagicMock()
        mock_bad_json.status_code = 200
        mock_bad_json.json.return_value = {"error": "no time here"}

        with patch("requests.get", return_value=mock_bad_json):
            with pytest.raises(ValueError, match="missing 'server_time'"):
                query_cristian_server("http://localhost:8000")

        # Case 2: Non-JSON response
        mock_bad_text = MagicMock()
        mock_bad_text.status_code = 200
        mock_bad_text.json.side_effect = ValueError("No JSON")
        mock_bad_text.text = "Internal error"

        with patch("requests.get", return_value=mock_bad_text):
            with pytest.raises(ValueError, match="not valid JSON"):
                query_cristian_server("http://localhost:8000")


class TestVectorClockMembershipAndValidation:
    """Tests for Vector Clock explicit membership management and validation (Phase 3)."""

    def test_add_and_remove_member(self):
        vc = VectorClock("P1", ["P1", "P2"])
        assert vc.get_members() == ["P1", "P2"]

        # Add member explicitly
        vc.add_member("P3")
        assert vc.get_members() == ["P1", "P2", "P3"]
        assert vc.get_clock()["P3"] == 0

        # Remove member explicitly
        vc.remove_member("P2")
        assert vc.get_members() == ["P1", "P3"]
        assert "P2" not in vc.get_clock()

        # Cannot retire self
        with pytest.raises(ValueError, match="cannot retire itself"):
            vc.remove_member("P1")

    def test_strict_membership_comparison_rejects_mismatch(self):
        v1 = {"P1": 1, "P2": 0}
        v2 = {"P1": 1, "P2": 0, "P3": 1}

        # Non-strict comparison allows unioned comparison
        rel = VectorClock.compare(v1, v2, strict_membership=False)
        assert rel in ("BEFORE", "CONCURRENT")

        # Strict comparison rejects mismatched process dimensions
        with pytest.raises(ValueError, match="Incompatible vector memberships"):
            VectorClock.compare(v1, v2, strict_membership=True)

    def test_strict_membership_receive_rejects_unregistered_process(self):
        vc = VectorClock("P1", ["P1", "P2"], strict_membership=True)
        # Receiving from unknown process P4 must be rejected under strict membership
        with pytest.raises(ValueError, match="Strict membership violation"):
            vc.receive_event({"P1": 1, "P4": 2})

    def test_invalid_vector_inputs(self):
        with pytest.raises(ValueError, match="Both arguments must be dictionary"):
            VectorClock.compare("invalid", {"P1": 1})


class TestLamportTraceRecorderAndCausalChain:
    """Tests for Lamport trace recording and strict causal ordering verification (Phase 3)."""

    def test_trace_recording_and_retrieval(self):
        from common.clocks import record_lamport_event, get_lamport_traces, clear_lamport_traces

        clear_lamport_traces()
        job_id = "test-job-causal-123"

        # Causal sequence: submit (L1) -> claim (L2) -> execute (L3) -> complete (L4)
        clock_sched = LamportClock()
        clock_worker = LamportClock()

        # Event 1: Submit on scheduler
        e1_ts = clock_sched.send_event()
        record_lamport_event("scheduler", "job_submitted", e1_ts, job_id=job_id)

        # Event 2: Claim on worker (receives e1)
        clock_worker.receive_event(e1_ts)
        e2_ts = clock_worker.get_time()
        record_lamport_event("worker-1", "job_claimed", e2_ts, job_id=job_id)

        # Event 3: Local execution on worker
        e3_ts = clock_worker.tick()
        record_lamport_event("worker-1", "job_executing", e3_ts, job_id=job_id)

        # Event 4: Complete and ACK
        e4_ts = clock_worker.send_event()
        record_lamport_event("worker-1", "job_completed", e4_ts, job_id=job_id)

        traces = get_lamport_traces(job_id=job_id)
        assert len(traces) == 4

        # Verify strict Lamport ordering: e1 -> e2 -> e3 -> e4 implies L(e1) < L(e2) < L(e3) < L(e4)
        ts_list = [t["lamport_ts"] for t in traces]
        assert ts_list[0] < ts_list[1] < ts_list[2] < ts_list[3]
        assert ts_list == [1, 2, 3, 4]
