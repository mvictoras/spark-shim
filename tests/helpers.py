"""Helpers for the spark-shim test suite."""

import importlib.machinery
import importlib.util
import os
import sys

SCRIPT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "spark-shim.sh")
)

_counter = [0]

# What the (faked) model discovery reports for the gateway.
DISCOVERED_MODELS = [
    "glm-5.3-flash-exl3",
    "anl-sophia/gpt-oss-120b",
    "anl-sophia/gpt-oss-20b",
    "anl-sophia/llama-3.3-70b",
]

DEFAULT_OPENCODE = {
    "provider": {
        "argo": {"options": {"baseURL": "https://127.0.0.1:17948/argoapi/v1"}},
        "spark": {
            "npm": "@ai-sdk/openai-compatible",
            "name": "Spark (local DGX)",
            "options": {
                "baseURL": "http://127.0.0.1:4100/v1",
                "headers": {
                    "Authorization": "Bearer sk-spark-anl-c72183474362a422"
                },
            },
            "models": {
                "glm-5.3-flash-exl3": {
                    "name": "GLM-5.3-Flash EXL3 (dual Spark, TP=2)",
                    "limit": {"context": 1048576, "output": 65536},
                    "variants": {
                        "none": {"reasoningEffort": "none"},
                        "low": {"reasoningEffort": "low"},
                        "medium": {"reasoningEffort": "medium"},
                        "high": {"reasoningEffort": "high"},
                        "max": {"reasoningEffort": "max"},
                    },
                }
            },
        },
    }
}

DEFAULT_HERMES = """\
# Hermes config — hand-edited, comments and ordering matter.
model:
  default: claudeopus48
  provider: custom:apps.inside.anl.gov
  base_url: https://apps.inside.anl.gov/argoapi/v1
  api_key: vmateevitsi

custom_providers:
  # argo, configured by hand
  - name: apps.inside.anl.gov
    base_url: https://apps.inside.anl.gov/argoapi/v1
    api_key: vmateevitsi
    api_mode: chat_completions
    model: claudeopus47
    context_length: 200000
    models:
      claudeopus47:
        context_length: 1000000

approvals:
  mode: ask
"""


class Recorder:
    """Records calls; usable directly as a fake process-spawning function."""

    def __init__(self, result=None):
        self.calls = []
        self.result = result

    def __call__(self, *args, **kwargs):
        self.calls.append(args)
        return self.result

    @property
    def called(self):
        return bool(self.calls)

    def args_of(self, nth=0):
        return self.calls[nth]


def load_module(home):
    """Load spark-shim.sh as a fresh module with HOME pointed at `home`.

    Every config path in the script is expanduser()'d at import time, so a
    fresh load per test with a temp HOME keeps the user's real configs safe.
    """
    counter = _counter[0]
    _counter[0] += 1
    old_home = os.environ.get("HOME")
    os.environ["HOME"] = str(home)
    try:
        # .sh extension: the source loader must be named explicitly.
        loader = importlib.machinery.SourceFileLoader(
            f"spark_shim_under_test_{counter}", SCRIPT
        )
        spec = importlib.util.spec_from_loader(loader.name, loader)
        mod = importlib.util.module_from_spec(spec)
        loader.exec_module(mod)
    finally:
        if old_home is not None:
            os.environ["HOME"] = old_home
    return mod


def fake_processes(mod, lab=True, fake_ensure_hermes=True):
    """Fake the process-spawning and probe boundary of a loaded module.

    Default world: the probe says `lab`, nothing is running, every
    start/stop succeeds and is recorded on the module as `mod.events`.
    Pass fake_ensure_hermes=False to keep the real Hermes tunnel logic
    (its own seams — pids, health, spawning — stay patchable).
    """
    mod.events = []
    mod.detect_direct = lambda api_key: lab

    def record(name, result=True):
        def fn(*args, **kwargs):
            mod.events.append((name,) + tuple(args))
            return result

        return fn

    mod.tunnel_pids = lambda port: []
    mod.loopback_relay_pids = lambda port: []
    mod.socat_pids = lambda port: []
    mod.ssh_relay_pids = lambda port: []
    mod.titan_ssh_relay_pids = lambda port: []
    mod.hermes_tunnel_pids = lambda: []
    mod.hermes_tunnel_healthy = lambda: True

    mod.start_tunnel = record("start_tunnel")
    mod.start_loopback_relay = record("start_loopback_relay")
    mod.start_relay = record("start_relay")
    mod.start_ssh_relay = record("start_ssh_relay")
    mod.start_titan_ssh_relay = record("start_titan_ssh_relay", result=True)
    if fake_ensure_hermes:
        mod.ensure_hermes_tunnel = record("ensure_hermes_tunnel")
    mod.stop_tunnel = record("stop_tunnel")
    mod.stop_loopback_relay = record("stop_loopback_relay")
    mod.stop_relay = record("stop_relay")
    mod.stop_ssh_relay = record("stop_ssh_relay")
    mod.stop_titan_ssh_relay = record("stop_titan_ssh_relay")
    mod.stop_hermes_tunnel = record("stop_hermes_tunnel")

    mod.gateway_responds = lambda *a, **k: True
    mod.fetch_models = lambda base_url, api_key: list(DISCOVERED_MODELS)
    return mod


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    import json

    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)
        fh.write("\n")


def read_json(path):
    import json

    with open(path) as fh:
        return json.load(fh)
