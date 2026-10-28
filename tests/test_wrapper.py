"""Seam 2: the argo-shim-all wrapper script.

Tests run the real wrapper with a stub `spark-shim` (its sibling in a fake
location, reached through a symlink from another directory) and a stub `uvx`
on a temporary PATH, asserting on the calls the stubs recorded: lab vs away
flags, no OpenCode Argo tunnel at the lab, the Argo shim backgrounded,
spark-shim called before Argo, and a failing step not blocking the rest.
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

WRAPPER = Path(__file__).resolve().parent.parent / "argo-shim-all.sh"

SPARK_STUB = """\
#!/bin/bash
echo "spark-shim $*" >> "$TEST_LOG"
if [ "${1:-}" = "--where" ]; then
  if [ -n "${TEST_WHERE_FAIL:-}" ]; then
    echo "probe exploded"
    exit 3
  fi
  echo "${TEST_WHERE:-away}"
  if [ "${TEST_WHERE:-away}" = "lab" ]; then exit 0; else exit 1; fi
fi
exit "${TEST_SPARK_EXIT:-0}"
"""

UVX_STUB = """\
#!/bin/bash
echo "uvx $*" >> "$TEST_LOG"
echo "CELS_USERNAME=$CELS_USERNAME" >> "$TEST_LOG"
case " $* " in
  *" --codex "*)
    # The real shim runs forever; prove the wrapper didn't wait for us.
    echo "shim-running $*" >> "$TEST_LOG"
    sleep 30
    echo "shim-exited" >> "$TEST_LOG"
    ;;
esac
exit "${TEST_UVX_EXIT:-0}"
"""


class WrapperTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="argo-shim-all-test-"))
        self.addCleanup(self.cleanup)

        # Fake "here" dir: the wrapper next to a stub spark-shim, exactly how
        # it finds the tool in the field (sibling of its own real path).
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        wrapper_copy = self.bin / "argo-shim-all.sh"
        shutil.copyfile(WRAPPER, wrapper_copy)
        wrapper_copy.chmod(0o755)
        (self.bin / "spark-shim.sh").write_text(SPARK_STUB)
        (self.bin / "uvx").write_text(UVX_STUB)
        for name in ("spark-shim.sh", "uvx"):
            (self.bin / name).chmod(0o755)

        self.home = self.tmp / "home"
        self.home.mkdir()
        self.log = self.tmp / "calls.log"
        # Run the wrapper through a symlink, from a different directory.
        self.link = self.tmp / "elsewhere"
        self.link.mkdir()
        self.symlink = self.link / "argo-shim-all.sh"
        self.symlink.symlink_to(wrapper_copy)

    def cleanup(self):
        # Kill any stubbed shim still sleeping in the background.
        subprocess.run(["pkill", "-f", str(self.tmp)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def env(self, **overrides):
        env = {
            "PATH": f"{self.bin}:/usr/bin:/bin",
            "HOME": str(self.home),
            "TEST_LOG": str(self.log),
            "TEST_WHERE": "away",
        }
        env.update({k: str(v) for k, v in overrides.items()})
        return env

    def run_wrapper(self, **env_overrides):
        return subprocess.run(
            ["bash", str(self.symlink)],
            env=self.env(**env_overrides),
            cwd=str(self.link),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=30,
        )

    def calls(self):
        return self.log.read_text().splitlines()

    def wait_for_log(self, substring, timeout=10):
        deadline = __import__("time").time() + timeout
        while __import__("time").time() < deadline:
            if self.log.exists() and substring in self.log.read_text():
                return
            __import__("time").sleep(0.1)
        self.fail(f"{substring!r} never appeared in {self.log}")

    # -- tests ---------------------------------------------------------------

    def test_away_opens_opencode_tunnel_then_backgrounds_shim(self):
        proc = self.run_wrapper(TEST_WHERE="away")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.wait_for_log("uvx argo-shim --restart --codex --port 17950")
        lines = self.calls()
        self.assertIn("uvx argo-shim --opencode", " ".join(lines))
        self.assertIn("uvx argo-shim --restart --codex --port 17950", " ".join(lines))
        # spark-shim ran before anything Argo-related.
        first_spark = next(
            i for i, line in enumerate(lines) if line.startswith("spark-shim")
        )
        first_argo = next(
            i for i, line in enumerate(lines) if line.startswith("uvx")
        )
        self.assertLess(first_spark, first_argo)
        # Backgrounded: the stub's sleep hadn't finished when the wrapper left.
        self.assertNotIn("shim-exited", self.log.read_text())
        self.assertIn("CELS_USERNAME=vmateevitsi", lines)

    def test_lab_runs_direct_and_skips_opencode_tunnel(self):
        proc = self.run_wrapper(TEST_WHERE="lab")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.wait_for_log("--codex")
        joined = " ".join(self.calls())
        self.assertIn("--direct", joined)
        self.assertIn("--codex", joined)
        self.assertNotIn("--opencode", joined)
        self.assertNotIn("shim-exited", self.log.read_text())

    def test_failing_spark_step_does_not_block_argo(self):
        proc = self.run_wrapper(TEST_SPARK_EXIT=1, TEST_WHERE="away")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("FAILED step", proc.stderr)
        self.wait_for_log("--codex")
        joined = " ".join(self.calls())
        self.assertIn("uvx argo-shim --opencode", joined)
        self.assertIn("--codex", joined)

    def test_where_failure_defaults_to_away(self):
        proc = self.run_wrapper(TEST_WHERE_FAIL=1, TEST_WHERE="away")
        self.assertNotEqual(proc.returncode, 0)  # the failure is reported
        self.assertIn("defaulting to away", proc.stderr)
        self.wait_for_log("--codex")
        joined = " ".join(self.calls())
        self.assertIn("uvx argo-shim --opencode", joined)
        self.assertIn("--codex", joined)

    def test_runs_from_other_directory_through_symlink(self):
        # setUp already runs via a symlink from another directory; if
        # resolution broke, spark-shim.sh would never be found and its stub
        # would never record.
        proc = self.run_wrapper(TEST_WHERE="lab")
        self.assertIn("spark-shim ", self.log.read_text())
        self.assertNotIn("No such file", proc.stderr)
        self.assertNotIn("command not found", proc.stderr)


if __name__ == "__main__":
    unittest.main()
