"""
Chandy-Lamport Distributed Snapshot Algorithm (Unit III — Synchronization).

Theoretical Background (K. Mani Chandy & Leslie Lamport, ACM TOCS 1985):
-------------------------------------------------------------------------
"Distributed Snapshots: Determining Global States of Distributed Systems"

In a distributed system without shared memory or a global clock, determining
a consistent global state (a "consistent cut") is essential for checkpointing,
deadlock detection, stable predicate evaluation, and distributed debugging.

System Model & Assumptions:
---------------------------
1. The distributed system is a directed graph of processes and unidirectional channels.
2. Channels are reliable (no message loss), error-free, and strictly FIFO (First-In, First-Out).
3. Process and channel graphs are strongly connected (every process can reach every other process).
4. Snapshot does not freeze the system: normal application message processing continues.

Algorithm Mechanics (Marker Rules):
-----------------------------------
1. Marker Sending Rule for Process P_i:
   - P_i records its own local state S_i.
   - For every outgoing channel C_ik, P_i sends a MARKER token before sending any further application messages.
   - P_i begins recording incoming messages for all other incoming channels.

2. Marker Receiving Rule for Process P_i along Channel C_ji:
   - If P_i has NOT yet recorded its state (first marker received):
     * P_i records its local state S_i.
     * P_i records the state of channel C_ji as EMPTY ([]).
     * P_i sends a MARKER along all outgoing channels C_ik.
     * P_i starts recording incoming messages on all incoming channels C_ki (k != j).
   - If P_i has ALREADY recorded its state:
     * P_i stops recording incoming messages on channel C_ji.
     * The recorded state of channel C_ji consists of all messages received on C_ji
       between the time P_i recorded its state and the time P_i received the marker on C_ji.

Consistent Cut Property:
------------------------
A global state S = (local states, channel states) is consistent if:
If event e_receive is in S, then its corresponding event e_send MUST also be in S.
No effect is observed without its cause.
"""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from common.logging_config import setup_logger

logger = setup_logger("snapshot")


@dataclass
class ChannelMessage:
    """Message passing through a unidirectional FIFO channel."""
    msg_type: str  # "APP" or "MARKER"
    sender: str
    receiver: str
    payload: Any = None
    marker_id: Optional[str] = None
    timestamp_iso: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __repr__(self) -> str:
        if self.msg_type == "MARKER":
            return f"[MARKER({self.marker_id}) from {self.sender} -> {self.receiver}]"
        return f"[APP({self.payload}) from {self.sender} -> {self.receiver}]"


class FIFOChannel:
    """Simulated reliable FIFO channel between two processes."""

    def __init__(self, sender: str, receiver: str) -> None:
        self.sender = sender
        self.receiver = receiver
        self.queue: deque[ChannelMessage] = deque()

    def send(self, message: ChannelMessage) -> None:
        self.queue.append(message)

    def receive(self) -> Optional[ChannelMessage]:
        return self.queue.popleft() if self.queue else None

    def peek(self) -> Optional[ChannelMessage]:
        return self.queue[0] if self.queue else None

    def has_messages(self) -> bool:
        return len(self.queue) > 0

    def __len__(self) -> int:
        return len(self.queue)


@dataclass
class GlobalSnapshotResult:
    """Recorded consistent global state."""
    marker_id: str
    process_states: Dict[str, Any]
    channel_states: Dict[str, List[Any]]  # Channel key "P1->P2" -> list of in-flight payloads
    is_complete: bool
    total_value: int


class SnapshotProcess:
    """A participant process in the Chandy-Lamport distributed snapshot protocol."""

    def __init__(self, process_id: str, initial_balance: int = 100) -> None:
        self.process_id = process_id
        self.balance = initial_balance

        # Channels: key is other process_id
        self.incoming_channels: Dict[str, FIFOChannel] = {}
        self.outgoing_channels: Dict[str, FIFOChannel] = {}

        # Snapshot recording structures for current marker
        self.active_marker_id: Optional[str] = None
        self.recorded_state: Optional[int] = None
        self.has_recorded_state: bool = False
        self.recording_channels: Set[str] = set()  # senders from which we record incoming messages
        self.recorded_channel_messages: Dict[str, List[Any]] = {}  # sender -> list of in-flight payloads
        self.markers_received_from: Set[str] = set()  # senders from which marker was received

        # Audit history of events
        self.history: List[str] = []

    def connect_incoming(self, sender_id: str, channel: FIFOChannel) -> None:
        self.incoming_channels[sender_id] = channel

    def connect_outgoing(self, receiver_id: str, channel: FIFOChannel) -> None:
        self.outgoing_channels[receiver_id] = channel

    def send_transfer(self, receiver_id: str, amount: int) -> bool:
        """Send application tokens to another process over FIFO channel."""
        if self.balance < amount or amount <= 0:
            return False
        if receiver_id not in self.outgoing_channels:
            return False

        self.balance -= amount
        msg = ChannelMessage(
            msg_type="APP",
            sender=self.process_id,
            receiver=receiver_id,
            payload=amount,
        )
        self.outgoing_channels[receiver_id].send(msg)
        self.history.append(f"SENT APP({amount}) to {receiver_id} (balance={self.balance})")
        logger.debug("[%s] Sent %d to %s (Remaining balance: %d)", self.process_id, amount, receiver_id, self.balance)
        return True

    def initiate_snapshot(self, marker_id: str = "SNAP-1") -> None:
        """Initiate Chandy-Lamport snapshot as the initiator process."""
        self.active_marker_id = marker_id
        # Step 1: Record local state
        self.recorded_state = self.balance
        self.has_recorded_state = True
        self.history.append(f"INITIATED SNAPSHOT: Recorded state={self.recorded_state}")
        logger.info("[%s] INITIATED Snapshot %s: Recorded state=%d", self.process_id, marker_id, self.recorded_state)

        # Step 2: Start recording on ALL incoming channels
        self.recording_channels = set(self.incoming_channels.keys())
        for sender in self.incoming_channels:
            self.recorded_channel_messages[sender] = []

        # Step 3: Broadcast MARKER along all outgoing channels
        self._broadcast_marker(marker_id)

    def _broadcast_marker(self, marker_id: str) -> None:
        """Send MARKER along all outgoing channels."""
        for receiver_id, channel in self.outgoing_channels.items():
            marker_msg = ChannelMessage(
                msg_type="MARKER",
                sender=self.process_id,
                receiver=receiver_id,
                marker_id=marker_id,
            )
            channel.send(marker_msg)
            self.history.append(f"SENT MARKER({marker_id}) to {receiver_id}")
            logger.debug("[%s] Sent MARKER to %s", self.process_id, receiver_id)

    def step(self) -> bool:
        """Process one incoming message from any incoming channel with messages.

        Returns True if a message was processed, False otherwise.
        """
        for sender_id, channel in self.incoming_channels.items():
            if channel.has_messages():
                msg = channel.receive()
                if msg is None:
                    continue
                self._handle_message(sender_id, msg)
                return True
        return False

    def _handle_message(self, sender_id: str, msg: ChannelMessage) -> None:
        """Handle incoming message according to Chandy-Lamport rules."""
        if msg.msg_type == "MARKER":
            self._handle_marker(sender_id, msg)
        elif msg.msg_type == "APP":
            self._handle_app_message(sender_id, msg)

    def _handle_marker(self, sender_id: str, msg: ChannelMessage) -> None:
        """Process incoming MARKER token."""
        marker_id = msg.marker_id or "SNAP-DEFAULT"
        self.markers_received_from.add(sender_id)

        if not self.has_recorded_state:
            # Case 1: First time this process has seen this marker!
            self.active_marker_id = marker_id
            # 1. Record local state
            self.recorded_state = self.balance
            self.has_recorded_state = True

            # 2. Record state of incoming channel C_{sender_id -> self} as EMPTY
            self.recorded_channel_messages[sender_id] = []

            # 3. Start recording on all other incoming channels
            self.recording_channels = set(self.incoming_channels.keys()) - {sender_id}
            for other_sender in self.recording_channels:
                self.recorded_channel_messages[other_sender] = []

            self.history.append(
                f"FIRST MARKER from {sender_id}: Recorded state={self.recorded_state}, "
                f"Channel({sender_id}->{self.process_id}) recorded as EMPTY"
            )
            logger.info(
                "[%s] Received first MARKER from %s. Recorded state=%d, Channel(%s->%s)=[]",
                self.process_id,
                sender_id,
                self.recorded_state,
                sender_id,
                self.process_id,
            )

            # 4. Forward MARKER along all outgoing channels
            self._broadcast_marker(marker_id)
        else:
            # Case 2: Process has already recorded its local state!
            # Stop recording incoming messages on this channel
            if sender_id in self.recording_channels:
                self.recording_channels.remove(sender_id)
            self.history.append(f"MARKER from {sender_id}: Stopped recording on Channel({sender_id}->{self.process_id})")
            logger.info(
                "[%s] Received subsequent MARKER from %s. Finalized in-flight on Channel(%s->%s): %s",
                self.process_id,
                sender_id,
                sender_id,
                self.process_id,
                self.recorded_channel_messages.get(sender_id, []),
            )

    def _handle_app_message(self, sender_id: str, msg: ChannelMessage) -> None:
        """Process application message and record if channel is currently being recorded."""
        amount = int(msg.payload) if msg.payload is not None else 0

        # If this channel is currently being recorded, message is in-flight along this channel!
        if sender_id in self.recording_channels:
            self.recorded_channel_messages[sender_id].append(amount)
            self.history.append(f"RECORDED IN-FLIGHT MSG APP({amount}) from {sender_id}")
            logger.info(
                "[%s] RECORDED in-transit message APP(%d) on channel %s->%s",
                self.process_id,
                amount,
                sender_id,
                self.process_id,
            )

        # Apply state transition
        self.balance += amount
        self.history.append(f"PROCESSED APP({amount}) from {sender_id} (balance={self.balance})")
        logger.debug("[%s] Received %d from %s (New balance: %d)", self.process_id, amount, sender_id, self.balance)

    def is_snapshot_complete(self) -> bool:
        """A process has completed its part when it recorded state and received markers on all incoming channels."""
        if not self.has_recorded_state:
            return False
        return self.markers_received_from == set(self.incoming_channels.keys())


class ChandyLamportCluster:
    """Manages a fully interconnected cluster of processes executing Chandy-Lamport snapshots."""

    def __init__(self, process_balances: Dict[str, int]) -> None:
        self.processes: Dict[str, SnapshotProcess] = {}
        self.channels: Dict[Tuple[str, str], FIFOChannel] = {}

        # 1. Instantiate processes
        for pid, bal in process_balances.items():
            self.processes[pid] = SnapshotProcess(process_id=pid, initial_balance=bal)

        # 2. Create fully connected directed channels between every pair of processes
        for p1 in self.processes:
            for p2 in self.processes:
                if p1 != p2:
                    chan = FIFOChannel(sender=p1, receiver=p2)
                    self.channels[(p1, p2)] = chan
                    self.processes[p1].connect_outgoing(p2, chan)
                    self.processes[p2].connect_incoming(p1, chan)

    def get_process(self, pid: str) -> SnapshotProcess:
        return self.processes[pid]

    def step_all(self, max_steps: int = 100) -> int:
        """Run simulation steps until no channels have pending messages or max_steps reached."""
        steps_taken = 0
        while steps_taken < max_steps:
            progress = False
            for proc in self.processes.values():
                if proc.step():
                    progress = True
                    steps_taken += 1
            if not progress:
                break
        return steps_taken

    def is_snapshot_complete(self) -> bool:
        return all(p.is_snapshot_complete() for p in self.processes.values())

    def get_global_snapshot(self, marker_id: str = "SNAP-1") -> GlobalSnapshotResult:
        """Aggregate recorded local states and in-transit channel states into a global snapshot."""
        proc_states = {}
        chan_states = {}
        total_tokens = 0

        for pid, p in self.processes.items():
            state = p.recorded_state if p.recorded_state is not None else p.balance
            proc_states[pid] = state
            total_tokens += state

            for sender_id, in_flight in p.recorded_channel_messages.items():
                key = f"{sender_id}->{pid}"
                chan_states[key] = list(in_flight)
                total_tokens += sum(in_flight)

        complete = self.is_snapshot_complete()
        return GlobalSnapshotResult(
            marker_id=marker_id,
            process_states=proc_states,
            channel_states=chan_states,
            is_complete=complete,
            total_value=total_tokens,
        )
