from __future__ import annotations

import subprocess
import sys
import threading
import unittest
from pathlib import Path
from typing import Any

from eventflow import BoundedWorkPool, EventBus
from eventflow.engine import EventEngine

from mavflow import MavApplicationPacket, MavEndpoint, MavRuntime
from mavflow.dispatch import MavApplicationDispatcher, MavApplicationHandlerRegistry


class _Router:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self.messages = EventBus[Any]()
        self.errors = EventBus[Exception]()


class _Connection:
    def __init__(self) -> None:
        self.errors = EventBus[Exception]()
        self.target_system = 1
        self.target_component = 1


class _Client:
    def __init__(self) -> None:
        self.endpoint = MavEndpoint()
        self.connection = _Connection()
        self.router = _Router()
        self.events = EventEngine()
        self.events.add("mavlink.messages", self.router.messages)
        self.events.add("mavlink.connection.errors", self.connection.errors)
        self.events.add("mavlink.router.errors", self.router.errors)
        self._lifecycle_lock = threading.RLock()
        self.is_connected = False

    @property
    def messages(self) -> EventBus[Any]:
        return self.router.messages

    def start(self) -> None:
        self.is_connected = True

    def stop(self) -> None:
        self.is_connected = False


class _Peer:
    def __init__(self) -> None:
        self.packets = EventBus[MavApplicationPacket]()
        self.changed = EventBus[Any]()
        self.errors = EventBus[Exception]()
        self._monitor_thread: threading.Thread | None = None

    def _accepts_dispatch_packet(self, _packet: MavApplicationPacket) -> bool:
        return True

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


def _run_lifecycle_worker(operation: str) -> None:
    if operation not in {"start", "reconnect"}:
        raise ValueError(f"Unsupported worker operation: {operation}")

    client = _Client()
    peer = _Peer()
    handler_entered = threading.Event()
    dispatcher_stop_entered = threading.Event()
    rejected = threading.Event()
    handler_errors: list[Exception] = []

    class ObservedDispatcher(MavApplicationDispatcher):
        def stop(self) -> None:
            dispatcher_stop_entered.set()
            super().stop()

    handlers = MavApplicationHandlerRegistry()
    runtime: MavRuntime

    def lifecycle_handler(_packet: MavApplicationPacket) -> None:
        handler_entered.set()
        if not dispatcher_stop_entered.wait(3.0):
            handler_errors.append(TimeoutError("external runtime stop did not reach dispatcher"))
            return
        try:
            getattr(runtime, operation)()
        except RuntimeError as exc:
            if "dispatcher worker thread" in str(exc):
                rejected.set()
            else:
                handler_errors.append(exc)
        except Exception as exc:
            handler_errors.append(exc)

    handlers.register("lifecycle.probe", lifecycle_handler)
    dispatcher = ObservedDispatcher(peer, handlers=handlers)  # type: ignore[arg-type]
    runtime = MavRuntime(
        client=client,  # type: ignore[arg-type]
        peer=peer,  # type: ignore[arg-type]
        dispatcher=dispatcher,
    )
    runtime.start()
    if not isinstance(dispatcher._work_pool, BoundedWorkPool):
        raise AssertionError("dispatcher did not start EventFlow's BoundedWorkPool")

    peer.packets.publish(MavApplicationPacket("lifecycle.probe", {}, packet_id=1))
    if not handler_entered.wait(2.0):
        raise TimeoutError("EventFlow worker did not enter the lifecycle handler")

    stop_errors: list[Exception] = []

    def stop_runtime() -> None:
        try:
            runtime.stop()
        except Exception as exc:
            stop_errors.append(exc)

    stopper = threading.Thread(target=stop_runtime, daemon=True)
    stopper.start()
    stopper.join(3.0)
    if stopper.is_alive():
        raise TimeoutError(f"runtime.stop deadlocked while worker called {operation}")
    if stop_errors:
        raise AssertionError(f"runtime.stop failed: {stop_errors!r}")
    if not rejected.is_set():
        raise AssertionError(f"runtime.{operation} was not rejected in its worker")
    if handler_errors:
        raise AssertionError(f"worker handler failed: {handler_errors!r}")
    if runtime.running or runtime._cleanup_pending:
        raise AssertionError("runtime shutdown did not complete cleanly")

    runtime.close()
    print(f"bounded-worker lifecycle guard passed: runtime.{operation}")


class ManagedWorkerLifecycleProcessTests(unittest.TestCase):
    def test_lifecycle_calls_from_real_eventflow_workers_do_not_deadlock(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        for operation in ("start", "reconnect"):
            with self.subTest(operation=operation):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "tests.test_lifecycle_subprocess",
                        "--worker",
                        operation,
                    ],
                    cwd=project_root,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=8.0,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    f"worker failed for runtime.{operation}\n"
                    f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
                )
                self.assertIn(f"runtime.{operation}", result.stdout)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        _run_lifecycle_worker(sys.argv[2])
    else:
        unittest.main()
