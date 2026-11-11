from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from benchmarks._runner import source_metadata

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class BenchmarkMetadataTests(unittest.TestCase):
    def test_router_fake_messages_use_their_mavlink_message_ids(self) -> None:
        from benchmarks.benchmark_router import _Message

        self.assertEqual(_Message(1, "HEARTBEAT").get_msgId(), 0)
        self.assertEqual(_Message(2, "ATTITUDE").get_msgId(), 30)

    def test_source_fingerprint_tracks_code_and_ignores_result_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            package = root / "mavflow"
            benchmark_directory = root / "benchmarks"
            results_directory = benchmark_directory / "results"
            package.mkdir()
            results_directory.mkdir(parents=True)
            (root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
            source = package / "__init__.py"
            source.write_text("value = 1\n", encoding="utf-8")
            (benchmark_directory / "benchmark_sample.py").write_text(
                "value = 1\n", encoding="utf-8"
            )

            before = source_metadata(root)
            (results_directory / "sample.json").write_text("{}\n", encoding="utf-8")
            after_result = source_metadata(root)
            source.write_text("value = 2\n", encoding="utf-8")
            after_source_change = source_metadata(root)

        self.assertEqual(
            before["mavflow_source_fingerprint_sha256"],
            after_result["mavflow_source_fingerprint_sha256"],
        )
        self.assertNotEqual(
            before["mavflow_source_fingerprint_sha256"],
            after_source_change["mavflow_source_fingerprint_sha256"],
        )

    def test_metadata_reports_source_identity_and_git_state(self) -> None:
        metadata = source_metadata(PROJECT_ROOT)

        self.assertRegex(metadata["mavflow_source_fingerprint_sha256"], FINGERPRINT_PATTERN)
        self.assertTrue(metadata["mavflow_git_revision"])
        self.assertIn(metadata["mavflow_source_dirty"], {"true", "false", "unknown"})


@unittest.skipUnless(importlib.util.find_spec("pyperf"), "pyperf development extra is required")
class PyperfRunnerTests(unittest.TestCase):
    def test_router_and_codec_write_pyperf_reports_with_workload_metadata(self) -> None:
        benchmarks = (
            (
                "benchmarks.benchmark_router",
                (
                    "--messages",
                    "10",
                    "--subscribers",
                    "4",
                    "--matching-ratio",
                    "50",
                ),
                {
                    "messages_per_batch": 10,
                    "subscriber_count": 4,
                    "matching_ratio_percent": 50,
                    "matching_messages_per_batch": 5,
                    "effective_matching_ratio_percent": 50.0,
                    "completion_observer_count": 1,
                    "timed_region": "message release through final envelope dispatch; setup, startup, and shutdown excluded",
                },
            ),
            (
                "benchmarks.benchmark_codec",
                ("--payload-bytes", "32"),
                {"payload_bytes": 32, "payload_shape": "flat"},
            ),
        )
        for module, workload_args, expected_metadata in benchmarks:
            with self.subTest(module=module), tempfile.TemporaryDirectory() as temporary_directory:
                output = Path(temporary_directory) / "benchmark.json"
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        module,
                        *workload_args,
                        "--processes=1",
                        "--values=1",
                        "--warmups=0",
                        "--loops=1",
                        "--output",
                        str(output),
                    ],
                    cwd=PROJECT_ROOT,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    f"pyperf smoke failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}",
                )
                report = json.loads(output.read_text(encoding="utf-8"))

            metadata = report["metadata"]
            for key, value in expected_metadata.items():
                self.assertEqual(metadata[key], value)
            self.assertRegex(metadata["mavflow_source_fingerprint_sha256"], FINGERPRINT_PATTERN)
            self.assertTrue(report["benchmarks"])


if __name__ == "__main__":
    unittest.main()
