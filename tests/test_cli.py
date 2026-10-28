"""Seam 1: the spark-shim CLI entry point.

Tests run main() against a temporary HOME with a fake reachability probe and
fakes at the process-spawning boundary (ssh tunnels, the relay daemon, the
Hermes forward). Assertions cover external behaviour only: files written,
processes the tool decided to start or stop, exit codes and printed output.
"""

import contextlib
import io
import os
import shutil
import socket
import tempfile
import threading
import unittest
from pathlib import Path

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from tests.helpers import (
    DEFAULT_HERMES,
    DEFAULT_OPENCODE,
    DISCOVERED_MODELS,
    Recorder,
    fake_processes,
    load_module,
    read_json,
    write_json,
)


class CLITestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="spark-shim-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.opencode_config = self.tmp / ".config/opencode/opencode.json"
        self.hermes_config = self.tmp / ".hermes/config.yaml"
        self.codex_config = self.tmp / ".codex/config.toml"
        self.state_dir = self.tmp / ".cache/spark-shim"
        write_json(self.opencode_config, DEFAULT_OPENCODE)
        self.hermes_config.parent.mkdir(parents=True, exist_ok=True)
        self.hermes_config.write_text(DEFAULT_HERMES)

    def load(self, lab=True, fake_ensure_hermes=True):
        mod = load_module(self.tmp)
        fake_processes(mod, lab=lab, fake_ensure_hermes=fake_ensure_hermes)
        return mod

    def run_main(self, mod, *argv):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            mod.main(list(argv))
        return out.getvalue()

    # -- location decision and the stable address --------------------------

    def test_away_uses_tunnel_and_stable_address(self):
        mod = self.load(lab=False)
        out = self.run_main(mod)
        self.assertIn("start_tunnel", [e[0] for e in mod.events])
        self.assertNotIn("start_loopback_relay", [e[0] for e in mod.events])
        cfg = read_json(self.opencode_config)
        self.assertEqual(
            cfg["provider"]["spark"]["options"]["baseURL"],
            "http://127.0.0.1:4100/v1",
        )
        # Hermes gets the same stable address and the tunnel is asked for.
        self.assertIn(("ensure_hermes_tunnel", "away"), mod.events)
        self.assertIn("http://127.0.0.1:4100/v1", self.hermes_config.read_text())
        self.assertIn("Restart OpenCode and Hermes", out)

    def test_lab_uses_relay_and_stable_address(self):
        mod = self.load(lab=True)
        self.run_main(mod)
        self.assertIn("start_loopback_relay", [e[0] for e in mod.events])
        self.assertNotIn("start_tunnel", [e[0] for e in mod.events])
        cfg = read_json(self.opencode_config)
        self.assertEqual(
            cfg["provider"]["spark"]["options"]["baseURL"],
            "http://127.0.0.1:4100/v1",
        )
        self.assertIn(("ensure_hermes_tunnel", "lab"), mod.events)

    def test_switch_from_away_to_lab_replaces_tunnel_with_relay(self):
        mod = self.load(lab=True)
        mod.tunnel_pids = lambda port: [4242]
        self.run_main(mod)
        self.assertIn(("stop_tunnel", 4100), mod.events)
        self.assertIn("start_loopback_relay", [e[0] for e in mod.events])

    def test_switch_from_lab_to_away_replaces_relay_with_tunnel(self):
        mod = self.load(lab=False)
        mod.loopback_relay_pids = lambda port: [1717]
        self.run_main(mod)
        self.assertIn(("stop_loopback_relay", 4100), mod.events)
        self.assertIn("start_tunnel", [e[0] for e in mod.events])

    def test_where_prints_and_exits_by_location(self):
        mod = self.load(lab=True)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            with self.assertRaises(SystemExit) as ctx:
                mod.main(["--where"])
        self.assertEqual(ctx.exception.code, 0)
        self.assertEqual(out.getvalue().strip(), "lab")
        mod2 = self.load(lab=False)
        out2 = io.StringIO()
        with contextlib.redirect_stdout(out2):
            with self.assertRaises(SystemExit) as ctx:
                mod2.main(["--where"])
        self.assertEqual(ctx.exception.code, 1)
        self.assertEqual(out2.getvalue().strip(), "away")

    def test_where_honours_manual_overrides(self):
        mod = self.load(lab=False)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            with self.assertRaises(SystemExit) as ctx:
                mod.main(["--where", "--direct"])
        self.assertEqual(ctx.exception.code, 0)
        self.assertEqual(out.getvalue().strip(), "lab")
        mod2 = self.load(lab=True)
        out2 = io.StringIO()
        with contextlib.redirect_stdout(out2):
            with self.assertRaises(SystemExit) as ctx:
                mod2.main(["--where", "--tunnel"])
        self.assertEqual(ctx.exception.code, 1)
        self.assertEqual(out2.getvalue().strip(), "away")

    # -- model discovery ----------------------------------------------------

    def test_discovery_preserves_tuning_and_adds_new_models(self):
        mod = self.load(lab=True)
        self.run_main(mod)
        models = read_json(self.opencode_config)["provider"]["spark"]["models"]
        # The pre-existing entry (limits and variants) is unchanged in meaning.
        self.assertEqual(
            models["glm-5.3-flash-exl3"],
            DEFAULT_OPENCODE["provider"]["spark"]["models"]["glm-5.3-flash-exl3"],
        )
        # New gateway models appear with minimal entries.
        for model_id in DISCOVERED_MODELS:
            self.assertIn(model_id, models)
        self.assertEqual(
            models["anl-sophia/gpt-oss-120b"], {"name": "anl-sophia/gpt-oss-120b"}
        )

    def test_default_model_seeded_with_variants_when_absent(self):
        mod = self.load(lab=True)
        cfg = read_json(self.opencode_config)
        del cfg["provider"]["spark"]["models"]["glm-5.3-flash-exl3"]
        write_json(self.opencode_config, cfg)
        self.run_main(mod)
        models = read_json(self.opencode_config)["provider"]["spark"]["models"]
        glm = models["glm-5.3-flash-exl3"]
        self.assertEqual(
            sorted(glm["variants"].keys()), ["high", "low", "max", "medium", "none"]
        )
        self.assertEqual(glm["limit"]["output"], 65536)

    def test_test_flag_uses_default_model(self):
        mod = self.load(lab=True)
        seen = []

        def fake_roundtrip(base_url, api_key, path, payload, headers):
            seen.append(payload["model"])
            return True, "ok"

        mod.roundtrip = fake_roundtrip
        out = self.run_main(mod, "--test")
        self.assertEqual(seen, ["glm-5.3-flash-exl3", "glm-5.3-flash-exl3"])
        # Both wire protocols get a model the gateway actually serves; the
        # old hard-coded names (spark/qwen3.8-27b, spark/gpt-oss-20b) never
        # go out on the wire.
        self.assertTrue(set(seen) <= set(DISCOVERED_MODELS))
        self.assertIn("Round-trip", out)

    def test_rerun_with_unchanged_models_is_semantically_stable(self):
        mod = self.load(lab=True)
        self.run_main(mod)
        first = read_json(self.opencode_config)
        self.run_main(mod)  # same gateway list, same config: no semantic drift
        second = read_json(self.opencode_config)
        self.assertEqual(first, second)

    # -- Hermes managed block ------------------------------------------------

    def test_hermes_block_inserted_as_first_item(self):
        mod = self.load(lab=True)
        self.run_main(mod)
        text = self.hermes_config.read_text()
        spark_pos = text.index("- name: spark")
        argo_pos = text.index("- name: apps.inside.anl.gov")
        self.assertLess(spark_pos, argo_pos)
        self.assertIn("custom_providers:", text[:spark_pos])
        # The rest of the file survives: Hermes's own model config untouched.
        self.assertIn("default: claudeopus48", text)
        self.assertIn("provider: custom:apps.inside.anl.gov", text)
        self.assertIn("# argo, configured by hand", text)
        if HAS_YAML:
            parsed = yaml.safe_load(text)
            providers = {p["name"]: p for p in parsed["custom_providers"]}
            spark = providers["spark"]
            self.assertEqual(spark["base_url"], "http://127.0.0.1:4100/v1")
            self.assertEqual(spark["api_mode"], "chat_completions")
            self.assertEqual(
                spark["models"]["glm-5.3-flash-exl3"]["context_length"], 1048576
            )
            # Discovered models have no known context length: no entry made up.
            self.assertNotIn(
                "context_length", spark["models"]["anl-sophia/gpt-oss-120b"]
            )
            self.assertNotIn("model", spark)  # Hermes's default model untouched
            self.assertEqual(parsed["model"]["default"], "claudeopus48")

    def test_hermes_block_replaced_on_rerun(self):
        mod = self.load(lab=True)
        self.run_main(mod)
        first = self.hermes_config.read_text()
        self.run_main(mod)  # same inputs: replace, not duplicate
        second = self.hermes_config.read_text()
        self.assertEqual(first, second)
        self.assertEqual(second.count("- name: spark"), 1)
        self.assertEqual(second.count("# >>> spark-shim >>>"), 1)

    def test_hermes_block_updated_when_models_change(self):
        mod = self.load(lab=True)
        self.run_main(mod)
        mod.fetch_models = lambda base_url, api_key: [
            "glm-5.3-flash-exl3",
            "brand-new/model",
        ]
        self.run_main(mod)
        text = self.hermes_config.read_text()
        self.assertIn("brand-new/model", text)
        # Models previously discovered stay (nothing is ever removed).
        self.assertIn("anl-sophia/gpt-oss-120b", text)
        self.assertEqual(text.count("# >>> spark-shim >>>"), 1)

    def test_hermes_section_absent_is_appended(self):
        mod = self.load(lab=True)
        self.hermes_config.write_text(
            "# only my hand-edited bits\nmodel:\n  default: claudeopus48\n"
        )
        self.run_main(mod)
        text = self.hermes_config.read_text()
        self.assertIn("custom_providers:", text)
        self.assertIn("- name: spark", text)
        self.assertIn("default: claudeopus48", text)
        if HAS_YAML:
            parsed = yaml.safe_load(text)
            self.assertEqual(parsed["custom_providers"][0]["name"], "spark")

    def test_hermes_no_backup_when_nothing_changed(self):
        mod = self.load(lab=True)
        self.run_main(mod)  # first run writes
        mod2 = self.load(lab=True)
        self.run_main(mod2)  # identical result: no-op
        backups = list(self.tmp.glob(".hermes/*.bak-spark-shim-*"))
        self.assertEqual(len(backups), 1)  # from the first run only

    def test_hermes_backup_written_before_change(self):
        mod = self.load(lab=True)
        original = self.hermes_config.read_text()
        self.run_main(mod)
        backups = list(self.tmp.glob(".hermes/*.bak-spark-shim-*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(), original)

    def test_no_hermes_flag_skips_hermes(self):
        mod = self.load(lab=True)
        self.run_main(mod, "--no-hermes")
        self.assertEqual(self.hermes_config.read_text(), DEFAULT_HERMES)
        self.assertNotIn("ensure_hermes_tunnel", [e[0] for e in mod.events])

    # -- Codex cleanup and Claude leftovers ----------------------------------

    def test_removed_flags_rejected_as_unknown(self):
        mod = self.load(lab=True)
        for flag in ("--claude", "--no-codex", "--restore-claude"):
            with self.subTest(flag=flag):
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as ctx:
                        mod.main([flag])
                self.assertEqual(ctx.exception.code, 2)  # argparse usage error

    def test_claude_settings_left_alone(self):
        mod = self.load(lab=True)
        claude_settings = self.tmp / ".claude/settings.json"
        write_json(claude_settings, {"env": {"ANTHROPIC_BASE_URL": "https://argo"}})
        original = claude_settings.read_text()
        self.run_main(mod)
        self.assertEqual(claude_settings.read_text(), original)
        self.assertEqual(list((self.tmp / ".claude").glob("*")), [claude_settings])

    def test_old_codex_block_stripped_with_backup(self):
        mod = self.load(lab=True)
        self.codex_config.parent.mkdir(parents=True, exist_ok=True)
        self.codex_config.write_text(
            "# my codex config\n\n"
            "# >>> spark-shim >>>\n"
            "[model_providers.spark]\n"
            'base_url = "http://127.0.0.1:4100/v1"\n'
            "# <<< spark-shim <<<\n"
            "\n[model_providers.other]\n"
        )
        self.run_main(mod)
        text = self.codex_config.read_text()
        self.assertNotIn("spark-shim", text)
        self.assertNotIn("model_providers.spark", text)
        self.assertIn("[model_providers.other]", text)
        backups = list(self.state_dir.glob("config.toml.bak-spark-shim-*"))
        self.assertEqual(len(backups), 1)
        self.assertIn("model_providers.spark", backups[0].read_text())

    def test_codex_untouched_without_old_block(self):
        mod = self.load(lab=True)
        self.codex_config.parent.mkdir(parents=True, exist_ok=True)
        self.codex_config.write_text("[model_providers.argo]\nname = 'argo'\n")
        self.run_main(mod)
        self.assertEqual(
            self.codex_config.read_text(), "[model_providers.argo]\nname = 'argo'\n"
        )
        self.assertEqual(list(self.state_dir.glob("config.toml.bak-*")), [])

    def test_claude_backup_left_alone(self):
        mod = self.load(lab=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        claude_backup = self.state_dir / "claude-settings.backup.json"
        claude_backup.write_text('{"env": {"ANTHROPIC_BASE_URL": "https://argo"}}')
        self.run_main(mod)
        self.assertEqual(
            claude_backup.read_text(),
            '{"env": {"ANTHROPIC_BASE_URL": "https://argo"}}',
        )

    # -- stop, status ---------------------------------------------------------

    def test_stop_tears_down_everything_we_own(self):
        mod = self.load(lab=True)
        self.run_main(mod, "--stop")
        names = [e[0] for e in mod.events]
        for expected in (
            "stop_tunnel",
            "stop_loopback_relay",
            "stop_relay",
            "stop_ssh_relay",
            "stop_titan_ssh_relay",
            "stop_hermes_tunnel",
        ):
            self.assertIn(expected, names)

    def test_status_warns_when_configured_address_has_no_listener(self):
        mod = self.load(lab=True)

        def no_local_listener(host, port, timeout=3.0):
            return host != "127.0.0.1"

        mod.port_open = no_local_listener
        out = self.run_main(mod, "--status")
        self.assertIn("nothing is listening on port 4100", out)
        self.assertIn("opencode", out)
        self.assertIn("hermes", out)
        self.assertNotIn("claude", out)
        self.assertNotIn("codex", out)

    def test_status_warns_on_path_location_mismatch(self):
        mod = self.load(lab=True)
        mod.tunnel_pids = lambda port: [999]
        out = self.run_main(mod, "--status")
        self.assertIn("away-style tunnel running, but this looks like the lab", out.lower())

    def test_status_shows_location_and_components(self):
        mod = self.load(lab=True)
        mod.loopback_relay_pids = lambda port: [1234]
        mod.hermes_tunnel_pids = lambda: [5678]
        out = self.run_main(mod, "--status")
        self.assertIn("lab", out)
        self.assertIn("Loopback relay", out)
        self.assertIn("Hermes tunnel", out)

    # -- Hermes dashboard tunnel: health and replacement -----------------------

    def test_hermes_tunnel_reused_when_healthy(self):
        import types

        mod = self.load(lab=False, fake_ensure_hermes=False)
        mod.hermes_tunnel_pids = lambda: [321]
        mod.hermes_tunnel_healthy = lambda: True
        spawn = Recorder(result=types.SimpleNamespace(returncode=0))
        mod.subprocess = types.SimpleNamespace(run=spawn, DEVNULL=-3)
        self.run_main(mod)
        self.assertFalse(spawn.called)

    def test_hermes_tunnel_dead_is_replaced(self):
        import types

        mod = self.load(lab=False, fake_ensure_hermes=False)
        mod.hermes_tunnel_pids = lambda: [321]
        mod.hermes_tunnel_healthy = lambda: False
        spawn = Recorder(result=types.SimpleNamespace(returncode=0))
        mod.subprocess = types.SimpleNamespace(run=spawn, DEVNULL=-3)
        out = self.run_main(mod)
        self.assertIn("dead", out.lower())
        self.assertIn(("stop_hermes_tunnel",), mod.events)
        self.assertTrue(spawn.called)
        cmd = spawn.args_of()[0]
        self.assertIn(
            f"127.0.0.1:9119:vr09.alcf.anl.gov:9119", " ".join(cmd)
        )
        self.assertIn("login-gce", cmd)
        self.assertIn("ControlMaster=no", cmd)  # dedicated, non-multiplexed

    def test_hermes_tunnel_not_started_at_lab(self):
        import types

        mod = self.load(lab=True, fake_ensure_hermes=False)
        spawn = Recorder(result=types.SimpleNamespace(returncode=0))
        mod.subprocess = types.SimpleNamespace(run=spawn, DEVNULL=-3)
        mod.hermes_tunnel_pids = lambda: [321]  # even a leftover is left alone
        self.run_main(mod)
        self.assertFalse(spawn.called)

    def test_hermes_tunnel_foreign_holder_left_alone(self):
        import types

        mod = self.load(lab=False, fake_ensure_hermes=False)
        mod.hermes_tunnel_pids = lambda: []  # not ours
        mod.port_open = lambda host, port, timeout=3.0: True  # but port held
        mod.port_holder = lambda port: "555 somebodyelse"
        spawn = Recorder(result=types.SimpleNamespace(returncode=0))
        mod.subprocess = types.SimpleNamespace(run=spawn, DEVNULL=-3)
        out = self.run_main(mod)
        self.assertFalse(spawn.called)
        self.assertIn("held by something else", out)

    def test_hermes_tunnel_spawn_failure_reported_not_fatal(self):
        import types

        mod = self.load(lab=False, fake_ensure_hermes=False)
        mod.hermes_tunnel_pids = lambda: []
        mod.port_open = lambda host, port, timeout=3.0: False
        spawn = Recorder(result=types.SimpleNamespace(returncode=255))
        mod.subprocess = types.SimpleNamespace(run=spawn, DEVNULL=-3)
        out = self.run_main(mod)  # must not raise
        self.assertIn("failed to open", out.lower())

    # -- publish mode untouched ----------------------------------------------

    def test_publish_mode_writes_hostname_and_skips_hermes(self):
        mod = self.load(lab=True)
        self.run_main(mod, "--publish")
        cfg = read_json(self.opencode_config)
        base = cfg["provider"]["spark"]["options"]["baseURL"]
        self.assertTrue(base.startswith("http://") and ":4100/v1" in base)
        self.assertNotIn("127.0.0.1", base)
        # Hermes is not touched in publish mode; no Mac-style path started.
        self.assertEqual(self.hermes_config.read_text(), DEFAULT_HERMES)
        names = [e[0] for e in mod.events]
        self.assertIn("start_relay", names)
        self.assertNotIn("start_loopback_relay", names)
        self.assertNotIn("start_tunnel", names)


class RelayOwnershipTestCase(unittest.TestCase):
    """The relay's port discipline: reuse our own, never touch anyone else's."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="spark-shim-relay-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.mod = load_module(self.tmp)

    def load(self):
        mod = load_module(self.tmp)
        fake_processes(mod, lab=True)
        return mod

    def test_healthy_relay_is_reused_not_duplicated(self):
        import types

        self.mod.loopback_relay_pids = lambda port: [123]
        self.mod.gateway_responds = lambda *a, **k: True
        spawn = Recorder()
        self.mod.subprocess = types.SimpleNamespace(Popen=spawn, DEVNULL=-3)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.mod.start_loopback_relay(4100, "sk-test")
        self.assertIn("Reusing existing relay", out.getvalue())
        self.assertFalse(spawn.called)  # no second relay spawned

    def test_ours_but_stale_relay_is_replaced(self):
        import types

        self.mod.loopback_relay_pids = lambda port: [123]
        answers = [False, True]  # stale first, healthy after the replace

        def fake_responds(*a, **k):
            return answers.pop(0)

        self.mod.gateway_responds = fake_responds
        probes = []

        def fake_port_open(host, port, timeout=3.0):
            probes.append(1)
            return len(probes) > 1  # not held, then listening

        self.mod.port_open = fake_port_open
        self.mod.stop_loopback_relay = Recorder(result=True)
        spawn = Recorder()
        self.mod.subprocess = types.SimpleNamespace(Popen=spawn, DEVNULL=-3)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.mod.start_loopback_relay(4100, "sk-test")
        self.assertIn("stale, replacing it", out.getvalue())
        self.assertIn((4100,), self.mod.stop_loopback_relay.calls)
        self.assertTrue(spawn.called)

    def test_foreign_holder_on_port_is_never_killed(self):
        import types

        self.mod.loopback_relay_pids = lambda port: []  # nothing of ours
        self.mod.port_open = lambda *a, **k: True  # but the port is held
        self.mod.port_holder = lambda port: "42 nobody evilproc"
        spawn = Recorder()
        self.mod.subprocess = types.SimpleNamespace(Popen=spawn, DEVNULL=-3)
        killed = Recorder()
        self.mod._signal_pid = killed
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as ctx:
                self.mod.start_loopback_relay(4100, "sk-test")
        self.assertEqual(ctx.exception.code, 1)
        self.assertFalse(spawn.called)
        self.assertFalse(killed.called)  # the holder is left alone

    def test_gateway_override_changes_relay_far_end(self):
        import types

        self.mod.loopback_relay_pids = lambda port: []
        probes = []

        def fake_port_open(host, port, timeout=3.0):
            probes.append(1)
            return len(probes) > 1  # not held, then listening

        self.mod.port_open = fake_port_open
        self.mod.gateway_responds = lambda *a, **k: True
        spawn = Recorder()
        self.mod.subprocess = types.SimpleNamespace(Popen=spawn, DEVNULL=-3)
        self.mod.apply_gateway("sparkmirror.example:5001")
        self.mod.start_loopback_relay(4100, "sk-test")
        cmd = spawn.args_of()[0]
        self.assertIn("--gateway", cmd)
        self.assertEqual(cmd[cmd.index("--gateway") + 1], "sparkmirror.example:5001")

    def test_spark_gateway_env_changes_relay_far_end(self):
        old = os.environ.get("SPARK_GATEWAY")
        os.environ["SPARK_GATEWAY"] = "sparkmirror.example:5001"
        self.addCleanup(os.environ.pop, "SPARK_GATEWAY", None)
        if old is not None:
            self.addCleanup(os.environ.__setitem__, "SPARK_GATEWAY", old)
        mod = self.load()
        mod.detect_direct = lambda api_key: True
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            with contextlib.suppress(SystemExit):
                mod.main(["--where"])
        self.assertEqual(out.getvalue().strip().splitlines()[-1], "lab")
        self.assertIn("Gateway override: sparkmirror.example:5001", out.getvalue())
        self.assertEqual(mod.GATEWAY_HOST, "sparkmirror.example")
        self.assertEqual(mod.GATEWAY_PORT, 5001)


class FarEndTestCase(unittest.TestCase):
    """The ssh side of the stable address: what the -L forwards actually dial."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="spark-shim-farend-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def test_tunnel_far_end_defaults_to_titan_loopback(self):
        mod = load_module(self.tmp)
        self.assertEqual(
            mod.tunnel_dest(), (mod.REMOTE_BIND, mod.DEFAULT_GATEWAY_PORT)
        )
        self.assertEqual(mod.forward_spec(4100), "4100:127.0.0.1:4000")

    def test_gateway_override_reroutes_tunnel_far_end(self):
        mod = load_module(self.tmp)
        mod.apply_gateway("sparkmirror.example:5000")
        self.assertEqual(mod.tunnel_dest(), ("sparkmirror.example", 5000))
        self.assertEqual(mod.forward_spec(4100), "4100:sparkmirror.example:5000")

    def test_away_tunnel_forwards_loopback_to_gateway_via_jump(self):
        import types

        mod = load_module(self.tmp)
        mod.tunnel_pids = lambda port: []
        probes = []

        def fake_port_open(host, port, timeout=3.0):
            probes.append((host, port))
            return len(probes) > 1  # not held, then listening

        mod.port_open = fake_port_open
        mod.gateway_responds = lambda *a, **k: True
        spawn = Recorder(result=types.SimpleNamespace(returncode=0))
        mod.subprocess = types.SimpleNamespace(run=spawn, DEVNULL=-3)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            mod.start_tunnel(4100, "sk-test")
        cmd = " ".join(spawn.args_of()[0])
        self.assertIn("127.0.0.1:4100:127.0.0.1:4000", cmd)
        self.assertIn("login-gce", cmd)  # the hop comes from the script
        self.assertIn("titan", cmd)  # ssh_config alias, not a hostname
        self.assertIn("Opening tunnel 127.0.0.1:4100", out.getvalue())

    def test_publish_relay_far_end_follows_the_gateway(self):
        mod = load_module(self.tmp)
        cmd = " ".join(mod.titan_ssh_relay_cmd(4100, batch=True))
        self.assertIn("0.0.0.0:4100:127.0.0.1:4000", cmd)
        self.assertIn("logins.cels.anl.gov", cmd)
        mod.apply_gateway("sparkmirror.example:5000")
        cmd = " ".join(mod.titan_ssh_relay_cmd(4100, batch=True))
        self.assertIn("0.0.0.0:4100:sparkmirror.example:5000", cmd)


class HermesHealthTestCase(unittest.TestCase):
    """The real health check: an HTTP response through the forward counts,
    a refusal or a silent close does not (the laptop-sleep leftover)."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="spark-shim-health-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.mod = load_module(self.tmp)

    def serve(self, behaviour):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(4)
        port = srv.getsockname()[1]

        def run():
            srv.settimeout(3)
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            if behaviour == "respond":
                conn.recv(256)
                conn.sendall(b"HTTP/1.1 404 Not Found\r\n\r\n")
            elif behaviour == "silent":
                pass  # accept and close without a byte
            conn.close()
            srv.close()

        threading.Thread(target=run, daemon=True).start()
        return port

    def test_any_http_response_counts_as_healthy(self):
        self.mod.HERMES_TUNNEL_PORT = self.serve("respond")
        self.assertTrue(self.mod.hermes_tunnel_healthy())

    def test_refused_connection_is_dead(self):
        self.mod.HERMES_TUNNEL_PORT = 1  # nothing listens on port 1
        self.assertFalse(self.mod.hermes_tunnel_healthy())

    def test_silent_close_is_dead(self):
        # The laptop-sleep leftover: ssh's listener accepts, then the
        # connection closes with no data because the far end is unreachable.
        self.mod.HERMES_TUNNEL_PORT = self.serve("silent")
        self.assertFalse(self.mod.hermes_tunnel_healthy())


if __name__ == "__main__":
    unittest.main()
