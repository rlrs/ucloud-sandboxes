import sys
import unittest
from tempfile import TemporaryFile

from ucloud_sandboxes.direct_service import DirectProcessRunner
from ucloud_sandboxes.sandbox import SandboxFileTooLargeError


class DirectProcessRunnerTests(unittest.TestCase):
    def test_file_stdin_is_consumed_without_materializing_it_in_python(self) -> None:
        with TemporaryFile() as source:
            source.write(b"binary\0payload" * 100000)
            source.seek(0)
            result = DirectProcessRunner().run(
                (sys.executable, "-c", "import sys; print(len(sys.stdin.buffer.read()))"),
                input_bytes=None, input_file=source, timeout_seconds=5,
                max_stdout_bytes=1024, max_stderr_bytes=1024,
            )
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, b"1400000\n")
            self.assertFalse(source.closed)

    def test_streams_stdin_and_captures_binary_output(self) -> None:
        result = DirectProcessRunner().run(
            (
                sys.executable,
                "-c",
                "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()[::-1])",
            ),
            input_bytes=b"\0abc",
            timeout_seconds=5,
            max_stdout_bytes=1024,
            max_stderr_bytes=1024,
        )
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, b"cba\0")

    def test_terminates_host_exec_when_output_bound_is_exceeded(self) -> None:
        with self.assertRaisesRegex(SandboxFileTooLargeError, "bounded output"):
            DirectProcessRunner().run(
                (
                    sys.executable,
                    "-c",
                    "import sys; sys.stdout.buffer.write(b'x' * 1048576)",
                ),
                input_bytes=None,
                timeout_seconds=5,
                max_stdout_bytes=1024,
                max_stderr_bytes=1024,
            )


if __name__ == "__main__":
    unittest.main()
