#!/usr/bin/env python3
# Copyright 2025 Google LLC

import argparse
from contextlib import contextmanager
from enum import Enum
import fcntl
import json
import os
from pathlib import Path
import pty
import re
import select
import signal
import struct
import subprocess
import sys
import termios
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

# ANSI color codes
RED = "\033[0;31m"
GREEN = "\033[0;32m"
CYAN = "\033[0;36m"
YELLOW = "\033[0;33m"
NC = "\033[0m"


class TestRunner(Enum):
    CATBOX = "catbox"
    ATEST = "atest"

    def __str__(self):
        return self.value


class TestResult:

    def __init__(
        self, test_name: str, success: bool, invocation_summary: Optional[str]
    ):
        self.test_name = test_name
        self.success = success
        self.invocation_summary = invocation_summary


class TestResults:

    def __init__(self):
        self.__results = list()

    def set(self, result: TestResult) -> None:
        self.__results.append(result)

    def extend(self, other: "TestResults") -> None:
        self.__results.extend(other.__results)

    def any_failed(self) -> bool:
        return any(not result.success for result in self.__results)

    def get_failed_test_names(self) -> List[str]:
        return [
            result.test_name for result in self.__results if not result.success
        ]

    def get_success_test_names(self) -> List[str]:
        return [result.test_name for result in self.__results if result.success]

    def get_invocation_summaries(self) -> List[str]:
        return [
            result.invocation_summary
            for result in self.__results
            if result.invocation_summary is not None
        ]


@contextmanager
def signal_handler(signum, handler):
    """A context manager to temporarily set a signal handler."""
    original_handler = signal.getsignal(signum)
    try:
        signal.signal(signum, handler)
        yield
    finally:
        signal.signal(signum, original_handler)


def print_large(text: str, color: str = YELLOW) -> None:
    """Prints a formatted separator with the given text."""
    print()
    print(f"{color}{'=' * 80}")
    print(f"{text}")
    print(f"{'=' * 80}{NC}")
    print()


def run_command(
    command: List[str],
    check: bool = True,
    env: Optional[Dict[str, str]] = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    """Runs a shell command.

    If `check` is `False`, its returncode will be ignored.
    """
    print(f"{CYAN}Executing: {' '.join(command)}{NC}")
    try:
        return subprocess.run(
            command,
            check=check,
            env=env,
            text=True,
            capture_output=capture_output,
        )
    except FileNotFoundError:
        print(f"{RED}Error: Command not found: {command[0]}{NC}")
        raise


def setup_devices(device_count: int) -> List[str]:
    """Sets up `device_count` Cuttlefish virtual devices."""
    print(f"Setting up {device_count} device(s)...")

    # Ensure a clean state before starting (these commands may fail if the state
    # is already clean, hence `check=False`)
    run_command(["cvd", "reset", "-y"], check=False)
    run_command(["cvd", "clear"], check=False)

    adb_serials: List[str] = list()
    for i in range(1, device_count + 1):
        result = run_command(
            ["cvd", "create", f"--config=sdv_core_instance{i}"],
            capture_output=True,
        )
        result = json.loads(result.stdout)
        adb_serials.append(result["instances"][0]["adb_serial"])
    return adb_serials


def print_summary(results: TestResults) -> None:
    """Prints a summary of all test runs."""
    print_large("Test Run Summary", color=CYAN)

    for test_name in results.get_success_test_names():
        print(f"- {test_name} {GREEN}SUCCESS{NC}")
    for test_name in results.get_failed_test_names():
        print(f"- {test_name} {RED}FAILURE{NC}")

    print()
    for invocation_summary in results.get_invocation_summaries():
        print(invocation_summary)


def parse_catbox_result(result_dir: Path) -> bool:
    """Parses the test_result.xml from catbox to determine whether the test succeeded."""
    xml_path = result_dir / "test_result.xml"
    if not xml_path.exists():
        print(f"{RED}Error: test_result.xml not found in {result_dir}{NC}")
        return False

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        failed_count = sum(
            int(s.get("failed", "0")) for s in root.findall("Summary")
        )
        modules_total = sum(
            int(s.get("modules_total", "0")) for s in root.findall("Summary")
        )
        modules_done = sum(
            int(s.get("modules_done", "0")) for s in root.findall("Summary")
        )
        incomplete_modules = modules_total - modules_done

        if failed_count == 0 and incomplete_modules == 0:
            return True

        print(
            f"{RED}{failed_count} tests failed and"
            f" {incomplete_modules} modules were incomplete.{NC}"
        )
        return False
    except ET.ParseError as e:
        print(f"{RED}Error parsing {xml_path}: {e}{NC}")
        return False


def stream_and_parse_command(
    command: List[str],
    env: Dict[str, str],
    stdout_line_regex: str,
) -> Tuple[int, Optional[str]]:
    """Runs a command and streams its output to the terminal.

    At the same time, the output is parsed by `stdout_line_regex`, which is
    executed on every line of output. Returns the returncode and the matched
    output, if found.
    """
    stdout_match: Optional[str] = None
    pty_parent, pty_child = pty.openpty()

    # Define a signal handler for terminal window size changes, which updates
    # the `pty_parent`'s window size on changes. This ensures that the fancy
    # rolling atest output works correctly.
    def _sigwinch_handler():
        """Signal handler for SIGWINCH to resize the PTY."""
        try:
            ts = os.get_terminal_size()
            winsize = struct.pack("HHHH", ts.lines, ts.columns, 0, 0)
            fcntl.ioctl(pty_parent, termios.TIOCSWINSZ, winsize)
        except OSError:
            # This can happen if the PTY is already closed.
            pass

    # Set the initial size of the PTY.
    _sigwinch_handler()

    with signal_handler(
        signal.SIGWINCH, lambda _signum, _frame: _sigwinch_handler()
    ), subprocess.Popen(
        command,
        stdin=pty_child,
        stdout=pty_child,
        stderr=pty_child,
        env=env,
    ) as process:
        os.close(pty_child)

        def _parse_and_update_match(line: str):
            nonlocal stdout_match
            if stdout_match:
                return
            match = re.search(stdout_line_regex, line)
            if match:
                stdout_match = match.group(1)

        # `buffer` is used to accumulate the program's output. It is necessary,
        # because the `os.read` call below may read less than a line.
        buffer = ""
        while True:
            try:
                # Wait until the pty_parent is ready to be read before
                # attempting to call `os.read`. Without this, atest seems to
                # hang for >10 seconds once it finishes.
                ready_to_read, _, _ = select.select([pty_parent], [], [], 0.1)
                if not ready_to_read:
                    # Timeout occurred, check if the process has exited.
                    if process.poll() is not None:
                        # Process has exited, break the loop.
                        break
                    else:
                        # Process is still running, wait for `ready_to_read`
                        # again.
                        continue

                data = os.read(pty_parent, 1024).decode("utf-8")
                if not data:  # An empty read means EOF.
                    break

                sys.stdout.write(data)

                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    _parse_and_update_match(line)

            except OSError:
                # This is expected when the child process closes the PTY.
                break
        os.close(pty_parent)

    # Process any remaining data in the buffer after the loop.
    _parse_and_update_match(buffer)

    return process.returncode, stdout_match


def run_test_with_catbox(
    test_name: str,
    device_count: int,
    adb_serials: List[str],
    catbox_binary: Path,
    catbox_dir: Path,
) -> Tuple[bool, Optional[Path], str]:
    """Runs a test using catbox."""

    serials = adb_serials[:device_count]
    test_env = os.environ.copy()
    test_env["NOTIFY_AS_NATIVE"] = ",".join(serials)

    catbox_args: List[str] = []
    for i, serial in enumerate(serials):
        catbox_args.extend([f"--{{device{i+1}}}serial", serial])

    returncode, log_file = stream_and_parse_command(
        command=[
            str(catbox_binary),
            "run",
            "commandAndExit",
            test_name,
        ]
        + catbox_args,
        env=test_env,
        stdout_line_regex=r"ProtoResultReporter: process final logs: (.+)",
    )
    if returncode != 0:
        # Catbox should always return with an exit code of 0, regardless of
        # whether the test failed or not. A non-0 exit code is akin to an infra
        # error.
        raise Exception(
            f"Catbox test execution failed (returncode: {returncode})."
        )

    test_result_dir = (Path(catbox_dir) / "results/latest").resolve()
    test_log_dir = Path(log_file).parent if log_file else None

    invocation_summary_path = test_result_dir / "invocation_summary.txt"

    return (
        parse_catbox_result(test_result_dir),
        test_log_dir,
        invocation_summary_path.read_text(),
    )


def run_test_with_atest(
    test_name: str,
    device_count: int,
    adb_serials: List[str],
) -> Tuple[bool, Optional[Path]]:
    serials = adb_serials[:device_count]
    test_env = os.environ.copy()
    test_env["NOTIFY_AS_NATIVE"] = ",".join(serials)

    # Convert test name from kebab-case to CamelCase for atest
    atest_test_name = re.sub(
        r"(?:^|-|(?<=\d))([a-z])",
        lambda x: x.group(1).upper(),
        test_name,
    )

    returncode, test_log_dir = stream_and_parse_command(
        command=["atest", atest_test_name],
        env=test_env,
        stdout_line_regex=r"Atest results and logs directory: (.+)",
    )
    return returncode == 0, Path(test_log_dir) if test_log_dir else None


def run_test_batch(
    device_count: int,
    test_names: List[str],
    test_runner: TestRunner,
    catbox_binary: Path,
    catbox_dir: Path,
) -> TestResults:
    """Runs a batch of tests that require the same number of devices."""
    results = TestResults()

    if not test_names:
        return results

    adb_serials = setup_devices(device_count)

    for test_num, test_name in enumerate(test_names):
        print_large(f"Running test: {test_name}")

        success = False
        test_log_dir: Optional[Path] = None
        invocation_summary: Optional[str] = None
        try:
            if test_runner == TestRunner.CATBOX:
                success, test_log_dir, invocation_summary = (
                    run_test_with_catbox(
                        test_name=test_name,
                        device_count=device_count,
                        adb_serials=adb_serials,
                        catbox_binary=catbox_binary,
                        catbox_dir=catbox_dir,
                    )
                )
            elif test_runner == TestRunner.ATEST:
                success, test_log_dir = run_test_with_atest(
                    test_name=test_name,
                    device_count=device_count,
                    adb_serials=adb_serials,
                )
        except subprocess.CalledProcessError:
            success = False

        if success:
            print_large(f"Test succeeded: {test_name}", color=GREEN)
            results.set(TestResult(test_name, True, invocation_summary))
        else:
            msg = f"Test failed: {test_name}"

            if test_log_dir is not None:
                msg += "\nDetailed test failure information:"
                msg += f"\n- Logs: {test_log_dir}"

            print_large(msg, color=RED)
            results.set(TestResult(test_name, False, invocation_summary))

            # Reset devices for the next test if this one failed
            if test_num < len(test_names) - 1:
                print("Resetting devices due to test failure...")
                adb_serials = setup_devices(device_count)

    return results


def get_device_count_for_test(
    test_name: str, sdv_catbox_config_dir: Path
) -> int:
    """Parses the test's XML config to determine the required device count."""
    config_path = sdv_catbox_config_dir / f"{test_name}.xml"
    if config_path.exists():
        tree = ET.parse(config_path)
        template = tree.find(".//template-include[@name='device-config']")
        if template is not None:
            device_config = template.get("default", "")
            if "one-device" in device_config:
                return 1
            if "two-devices" in device_config:
                return 2
            if "three-devices" in device_config:
                return 3

    raise Exception(f"Unable to determine device count for test: {test_name}")


def run_all_tests(
    test_names: List[str],
    test_runner: TestRunner,
    catbox_dir: Path,
    catbox_binary: Path,
    sdv_catbox_config_dir: Path,
) -> None:
    """Categorizes and runs all specified tests."""
    # Build necessary components
    if test_runner == TestRunner.CATBOX:
        run_command(["m", "catbox"])

    all_results = TestResults()

    # Categorize tests by device count
    test_batches: Dict[int, List[str]] = {}
    for test_name in test_names:
        device_count = get_device_count_for_test(
            test_name, sdv_catbox_config_dir
        )
        test_batches.setdefault(device_count, []).append(test_name)

    # Run tests
    for device_count, test_names in test_batches.items():
        batch_results = run_test_batch(
            device_count, test_names, test_runner, catbox_binary, catbox_dir
        )
        all_results.extend(batch_results)

    print_summary(all_results)

    if all_results.any_failed():
        try:
            if sys.stdin.isatty():
                # Clear potential garbage in stdin buffer.
                termios.tcflush(sys.stdin, termios.TCIFLUSH)

            print()
            print()
            reply = input("Some tests failed. Rerun them? (y/N) ").lower()
            if reply == "y":
                print_large("Rerunning failed tests...")
                run_all_tests(
                    test_names=all_results.get_failed_test_names(),
                    test_runner=test_runner,
                    catbox_dir=catbox_dir,
                    catbox_binary=catbox_binary,
                    sdv_catbox_config_dir=sdv_catbox_config_dir,
                )
        except (EOFError, KeyboardInterrupt):
            # Handle cases where stdin is not available or user interrupts
            print("Skipping rerun prompt.")


def main() -> None:
    """Main entry point of the script."""
    if "ANDROID_BUILD_TOP" not in os.environ:
        print(
            f"{RED}ANDROID_BUILD_TOP is missing. Please lunch the Android"
            f" target.{NC}"
        )
        sys.exit(1)

    android_build_top = Path(os.environ["ANDROID_BUILD_TOP"])
    out_dir = os.environ["OUT_DIR"]

    catbox_dir = (
        android_build_top / out_dir / "host/linux-x86/catbox/android-catbox"
    )
    catbox_binary = catbox_dir / "tools/catbox-tradefed"

    sdv_e2e_test_repo = (
        android_build_top / "vendor/google_testing/software_defined_vehicle"
    )
    sdv_catbox_config_dir = sdv_e2e_test_repo / "tools/sdv_catbox/res/config"

    parser = argparse.ArgumentParser(
        description="Run SDV Telemetry end-to-end tests."
    )
    parser.add_argument(
        "--test-runner",
        type=TestRunner,
        choices=list(TestRunner),
        default=TestRunner.CATBOX,
        help="The test runner to use.",
    )
    parser.add_argument(
        "test_names",
        nargs="*",
        help=(
            "Specific test names to run. If not provided, all Telemetry tests"
            " are run."
        ),
    )
    args = parser.parse_args()

    if args.test_names:
        test_names: List[str] = args.test_names
    else:
        # Find all telemetry test configs and extract their basenames
        test_names = sorted([
            p.stem for p in sdv_catbox_config_dir.glob("sdv-*-telemetry-*.xml")
        ])

    run_all_tests(
        test_names=test_names,
        test_runner=args.test_runner,
        catbox_dir=catbox_dir,
        catbox_binary=catbox_binary,
        sdv_catbox_config_dir=sdv_catbox_config_dir,
    )


if __name__ == "__main__":
    main()
