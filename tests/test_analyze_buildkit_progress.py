import json
from pathlib import Path
import tempfile
import unittest

from scripts.analyze_buildkit_progress import analyze_directory, parse_progress


class BuildkitProgressTests(unittest.TestCase):
    def test_nested_export_timings_and_repeated_vertices_do_not_double_count(self):
        result = parse_progress("""#0 building with "test" instance using docker-container driver
#1 [internal] load build context
#1 DONE 0.0s
#2 [build 2/4] RUN npm run lint && npm run build && npm test
#2 0.100 private command output https://user:password@example.invalid/secret
#1 [internal] load build context
#1 DONE 0.1s
#2 DONE 29.4s
#3 exporting to image
#3 exporting layers 0.9s done
#3 exporting layers 0.9s done
#3 pushing layers 0.1s done
#3 pushing manifest for private.invalid/owned:tag 0.0s done
#3 DONE 1.0s
#4 exporting cache to registry
#4 writing layer sha256:aaa done
#4 writing layer sha256:bbb 0.1s done
#4 sending cache export 0.2s done
#4 DONE 0.2s
""")
        self.assertEqual(result["completed_vertex_seconds_by_category"], {
            "internal": .1, "run": 29.4, "image_export": 1.0, "cache_export": .2})
        self.assertEqual(result["sub_operations"]["export_layers"]["timed_seconds"]["count"], 1)
        self.assertEqual(result["sub_operations"]["write_cache_layer"]["untimed_completions"], 1)
        self.assertEqual(result["vertices"][0]["terminal_observations"], 2)
        self.assertEqual(result["vertices"][1]["fixture_activity"], "node_lint_build_test_smoke")
        serialized = json.dumps(result)
        for secret in ("password", "example.invalid", "private.invalid", "sha256:aaa", "npm run", "command output"):
            self.assertNotIn(secret, serialized)

    def test_truncated_tail_cached_unknown_incomplete_and_errors_are_distinct(self):
        result = parse_progress("""partial output
[output truncated; showing retained tail]
#8 DONE 8.0s
#9 [build 4/7] RUN npm ci --private-secret
#9 CACHED
#10 [build 5/7] COPY secret-path ./
#11 importing cache manifest from https://private.invalid/cache
#11 ERROR: unexpected private response
#12 exporting to image
#12 CANCELED
""")
        # BuildKit normally uses ERROR: text; status parsing must not retain it.
        self.assertEqual(result["coverage"]["explicit_truncation_markers"], 1)
        self.assertFalse(result["coverage"]["first_line_is_progress"])
        self.assertFalse(result["coverage"]["complete_capture_proven"])
        self.assertEqual(result["completed_vertex_seconds_by_category"], {"unknown": 8.0})
        self.assertEqual(result["status_counts"]["cached"], 1)
        self.assertEqual(result["status_counts"]["canceled"], 1)
        self.assertEqual(result["status_counts"]["error"], 1)
        self.assertEqual(result["coverage"]["incomplete_vertices"], 1)
        self.assertNotIn("private", json.dumps(result))

    def test_export_stdout_cannot_spoof_header_or_timing(self):
        result = parse_progress("""#1 [stage 1/1] RUN arbitrary command
#1 0.1 exporting to image
#1 0.2 DONE 900.0s
#1 DONE 2.0s
#2 exporting to image
#2 unpacking to private.invalid/repo 0.3s done
#2 DONE 0.5s
""")
        self.assertEqual(result["completed_vertex_seconds_by_category"], {"run": 2.0, "image_export": .5})
        self.assertEqual(result["sub_operations"]["unpack"]["timed_seconds"]["sum"], .3)

    def test_cached_run_materialization_is_not_reported_as_command_execution(self):
        digest = "a" * 64
        result = parse_progress(f"""#14 [6/8] RUN python -m pip install --no-build-isolation ./native
#14 CACHED
#14 sha256:{digest} 50.00MB / 100.00MB 0.5s
#14 sha256:{digest} 100.00MB / 100.00MB 1.1s done
#14 extracting sha256:{digest} 3.1s done
#14 DONE 6.0s
#14 [6/8] RUN python -m pip install --no-build-isolation ./native
#14 DONE 13.7s
""")
        self.assertEqual(result["completed_vertex_seconds_by_category"], {"cached_layer_materialization": 13.7})
        vertex = result["vertices"][0]
        self.assertEqual(vertex["terminal_statuses_seen"], ["cached", "done"])
        self.assertEqual(vertex["reported_layer_transfer_bytes"], 100_000_000)
        self.assertEqual(vertex["fixture_activity"], "python_native_extension")
        self.assertNotIn(digest, json.dumps(result))

    def test_materialization_plus_stdout_remains_ambiguous(self):
        result = parse_progress("#1 [1/1] RUN command\n#1 extracting sha256:abc 1.0s done\n#1 1.0 output\n#1 DONE 2.0s\n")
        self.assertEqual(result["completed_vertex_seconds_by_category"], {"mixed_execution_materialization": 2.0})

    def test_receipt_join_is_allowlisted_and_missing_logs_are_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            image = "bl20260929-exec-repeat-007"
            (root / (image + ".build.log")).write_text("#1 [build 1/1] RUN npm run lint && npm run build && npm test\n#1 DONE 3.0s\n")
            report = analyze_directory(root, {"records": [
                {"image_id": image, "recipe": "typescript-tools", "variant": "app-change-7",
                 "token": "credential-must-not-escape", "submission_seconds": 5,
                 "build": {"timings": {"phases": {"docker_build_and_push_ms": 4000}}}},
                {"image_id": "bl20260929-exec-repeat-008", "recipe": "typescript-tools"},
            ]})
            self.assertEqual(report["builds"], 1)
            self.assertEqual(report["missing_logs"], ["bl20260929-exec-repeat-008"])
            self.assertEqual(report["recipes"]["typescript-tools"]["per_build_completed_vertex_seconds"]["run"]["mean"], 3)
            self.assertEqual(report["records"][0]["fixture"]["docker_build_and_push_seconds"], 4)
            self.assertNotIn("credential", json.dumps(report))
            with self.assertRaises(ValueError):
                analyze_directory(root, {"records": [{"image_id": "../../secret"}]})


if __name__ == "__main__":
    unittest.main()
