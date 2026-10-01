#!/usr/bin/env python3
"""spark-shim — reach the DGX LiteLLM gateway on `titan` from anywhere.

Unlike argo-shim, this needs no HTTP translation proxy. The LiteLLM gateway on
titan:4000 already speaks BOTH wire protocols natively:

    /v1/chat/completions   OpenAI    -> opencode, codex
    /v1/messages           Anthropic -> claude code (streaming SSE included)

So the only real problem is reachability: `titan.alcf.anl.gov` is an
internal-only name (NXDOMAIN off-site), and the gateway binds a private
network. On the ALCF network you can hit it directly; from home you must hop
through the CELS gateway. SSH handles that fine, but opencode/claude/codex
speak HTTP, not SSH.

This script bridges that gap: it puts a plain local TCP port in front of the
gateway and rewrites each client's config to point at it.

    home         ->  127.0.0.1:4100  ==ssh -J login-gce==>  titan:4000
    ALCF login   ->  titan.alcf.anl.gov:4000  (direct, no tunnel)
    ALCF compute ->  http://<login-node>:4100  (socat relay via --publish)

Location is auto-detected, so the same command works in both places. ALCF
compute nodes cannot reach titan (or the internet) directly, so --publish
runs on a login node: it exposes a socat relay on 0.0.0.0 and rewrites the
shared-home client configs to <login-node>:<port>. Compute nodes need
nothing installed -- they just use the shared configs.

Usage:
    spark-shim                  # connect + configure opencode & codex
    spark-shim --publish        # ALCF login node: socat relay for compute nodes
    spark-shim --gateway h:p    # target a different gateway endpoint
    spark-shim --claude         # ...and repoint Claude Code (displaces argo)
    spark-shim --status         # what's running, what's configured
    spark-shim --test           # real round-trip on both protocols
    spark-shim --models         # list models the gateway is serving
    spark-shim --stop           # tear the tunnel/relay down
    spark-shim --restore-claude # put argo's Claude Code settings back
"""

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

# --- topology ---------------------------------------------------------------
# `titan` and `login-gce` are ssh_config aliases, NOT hostnames. Usernames,
# keys and the ProxyJump chain live in ~/.ssh/config and are deliberately not
# duplicated here, so fixing SSH in one place fixes it everywhere.
SSH_HOST = "titan"
SSH_JUMP = "login-gce"
REMOTE_BIND = "127.0.0.1"   # gateway listens on 0.0.0.0, but loopback-from-titan
                            # is the shortest path once we're on the box
DEFAULT_GATEWAY_HOST = "titan.alcf.anl.gov"
DEFAULT_GATEWAY_PORT = 4000
GATEWAY_HOST = DEFAULT_GATEWAY_HOST   # override with --gateway or $SPARK_GATEWAY
GATEWAY_PORT = DEFAULT_GATEWAY_PORT

DEFAULT_PORT = 4100
FALLBACK_KEY = "sk-spark-anl-c72183474362a422"

# Sensible defaults for clients that must name a model up front.
BIG_MODEL = "spark/qwen3.8-27b"
SMALL_MODEL = "spark/gpt-oss-20b"

OPENCODE_CONFIG = os.path.expanduser("~/.config/opencode/opencode.json")
CLAUDE_SETTINGS = os.path.expanduser("~/.claude/settings.json")
CODEX_CONFIG = os.path.expanduser("~/.codex/config.toml")
STATE_DIR = os.path.expanduser("~/.cache/spark-shim")
CLAUDE_BACKUP = os.path.join(STATE_DIR, "claude-settings.backup.json")

CODEX_BEGIN = "# >>> spark-shim >>>"
CODEX_END = "# <<< spark-shim <<<"

OK, BAD, WARN, DOT = "\u2713", "\u2717", "\u26a0", "\u2192"


def say(msg=""):
    print(msg, flush=True)


def die(msg, code=1):
    say(f"{BAD} {msg}")
    sys.exit(code)


# --- reachability -----------------------------------------------------------

def port_open(host, port, timeout=3.0):
    """True if a TCP connection to host:port completes."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def gateway_responds(host, port, api_key, timeout=8):
    """True if host:port answers as the LiteLLM gateway (not some other service).

    Authenticates, because an unauthenticated probe returns 401 from both a
    healthy gateway and, potentially, an unrelated service. Requiring a valid
    model list is what makes this a positive identification.
    """
    req = urllib.request.Request(
        f"http://{host}:{port}/v1/models",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode())
        return any("id" in m for m in body.get("data", []))
    except Exception:
        return False


def detect_direct(api_key):
    """True when the gateway is reachable without a tunnel (i.e. on ALCF net)."""
    if not port_open(GATEWAY_HOST, GATEWAY_PORT, timeout=3.0):
        return False
    return gateway_responds(GATEWAY_HOST, GATEWAY_PORT, api_key)


def apply_gateway(spec):
    """Point GATEWAY_HOST/GATEWAY_PORT at `spec` ("host[:port]", scheme optional)."""
    global GATEWAY_HOST, GATEWAY_PORT
    spec = spec.strip()
    for scheme in ("http://", "https://"):
        if spec.startswith(scheme):
            spec = spec[len(scheme):]
            break
    spec = spec.rstrip("/")
    host, _, port = spec.partition(":")
    if not host or (port and not port.isdigit()):
        die(f"Invalid gateway endpoint {spec!r} (expected host[:port])")
    GATEWAY_HOST = host
    GATEWAY_PORT = int(port) if port else DEFAULT_GATEWAY_PORT


# --- tunnel lifecycle -------------------------------------------------------

def tunnel_dest():
    """Where the ssh-side end of the -L forward points.

    Default is the gateway's loopback on the jump box (shortest path); a
    --gateway override reroutes the far end to the configured host instead.
    """
    if GATEWAY_HOST != DEFAULT_GATEWAY_HOST:
        return GATEWAY_HOST, GATEWAY_PORT
    return REMOTE_BIND, GATEWAY_PORT


def forward_spec(port):
    dest_host, dest_port = tunnel_dest()
    return f"{port}:{dest_host}:{dest_port}"


def tunnel_pids(port):
    """PIDs of our own ssh tunnels for `port`.

    Matched by the -L forward spec *and* the ssh_config alias, so we never kill
    an unrelated process that merely happens to hold the port.
    """
    pids = []
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=5,
        ).stdout
    except Exception:
        return pids
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True, text=True, timeout=5,
            ).stdout
        except Exception:
            continue
        if "ssh" in args and forward_spec(port) in args and SSH_HOST in args:
            pids.append(int(pid))
    return pids


def port_holder(port):
    """Human-readable description of whatever is holding `port`, or None."""
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=5,
        ).stdout
        for pid in filter(None, (p.strip() for p in out.splitlines())):
            return subprocess.run(
                ["ps", "-o", "pid=,user=,comm=", "-p", pid],
                capture_output=True, text=True, timeout=5,
            ).stdout.strip()
    except Exception:
        pass
    return None


def stop_tunnel(port, quiet=False):
    """Stop our tunnel on `port` and confirm the port is actually released.

    Returns True only once nothing is listening. A tunnel that is wedged
    (suspended, or blocked in the kernel) ignores SIGTERM and keeps the port
    bound, so termination is verified and escalated rather than assumed --
    otherwise the caller sees the port still held and blames an innocent
    process.
    """
    pids = tunnel_pids(port)
    if not pids:
        if not quiet:
            say(f"  No spark-shim tunnel on port {port}")
        return not port_open("127.0.0.1", port, timeout=1.0)
    return _terminate_listeners(pids, port, quiet, "tunnel")


def _signal_pid(pid, sig):
    """Send `sig` to `pid`; True if the process existed."""
    try:
        os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return False
    except OSError as exc:
        say(f"  {WARN} Could not signal PID {pid}: {exc}")
        return False


# --- socat relay (ALCF publish mode) ----------------------------------------

def socat_pids(port):
    """PIDs of our socat relays listening for `port`.

    Matched by the listen spec in the process args, so we never kill an
    unrelated socat that merely happens to hold the port.
    """
    pids = []
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=5,
        ).stdout
    except Exception:
        return pids
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True, text=True, timeout=5,
            ).stdout
        except Exception:
            continue
        if "socat" in args and f"TCP-LISTEN:{port}," in args:
            pids.append(int(pid))
    return pids


def _terminate_listeners(pids, port, quiet, label):
    """SIGTERM (then SIGKILL) `pids` and confirm `port` is actually released.

    A wedged process ignores SIGTERM and keeps the port bound, so termination
    is verified and escalated rather than assumed -- otherwise the caller sees
    the port still held and blames an innocent process.
    """
    for sig in (15, 9):
        alive = [p for p in pids if _signal_pid(p, sig)]
        # SIGKILL can't be caught, but the port is released asynchronously.
        for _ in range(10):
            if not port_open("127.0.0.1", port, timeout=1.0):
                if not quiet:
                    say(f"  {OK} Stopped {label} (PID {', '.join(map(str, pids))})")
                return True
            time.sleep(0.3)
        if alive and sig == 15 and not quiet:
            say(f"  {WARN} {label} ignored SIGTERM, forcing it down")
    if not quiet:
        say(f"  {BAD} Port {port} still held after SIGKILL")
    return False


def stop_relay(port, quiet=False):
    """Stop our socat relay on `port`; True once nothing is listening."""
    pids = socat_pids(port)
    if not pids:
        if not quiet:
            say(f"  No spark-shim relay on port {port}")
        return not port_open("127.0.0.1", port, timeout=1.0)
    return _terminate_listeners(pids, port, quiet, "relay")


def start_relay(port, dest_host, dest_port):
    """Ensure a socat relay 0.0.0.0:{port} -> {dest_host}:{dest_port} is running.

    socat, not ssh: on an ALCF login node the gateway is reachable directly,
    so a plain TCP relay is the minimal moving part -- no keys, no agent, and
    no CSPO lockout risk from failed SSH auths on a shared machine.
    """
    if not shutil.which("socat"):
        die("socat not found in PATH; publish mode needs it to expose the "
            "gateway on 0.0.0.0.")
    if socat_pids(port):
        say(f"  {OK} Reusing existing relay on 0.0.0.0:{port}")
        return
    if port_open("127.0.0.1", port, timeout=1.0):
        holder = port_holder(port) or "unknown process"
        die(f"Port {port} is held by something else:\n    {holder}\n"
            f"  Use --port <PORT> to pick another.")

    cmd = ["socat", f"TCP-LISTEN:{port},fork,reuseaddr,bind=0.0.0.0",
           f"TCP:{dest_host}:{dest_port}"]
    say(f"  Publishing 0.0.0.0:{port} {DOT} {dest_host}:{dest_port} (socat)")
    # start_new_session detaches the relay from this shell so it survives
    # logout, the way `nohup ... &` would; stdio to devnull so no pipe can
    # hold a reader open.
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(20):
        if port_open("127.0.0.1", port, timeout=1.0):
            break
        if proc.poll() is not None:
            die(f"socat exited with code {proc.returncode}; is port {port} free?")
        time.sleep(0.5)
    else:
        stop_relay(port, quiet=True)
        die(f"Relay on port {port} never started listening")
    say(f"  {OK} Relay published on 0.0.0.0:{port}")


def start_tunnel(port, api_key):
    """Ensure a working tunnel on `port`; reuse a healthy one if present."""
    if tunnel_pids(port):
        if gateway_responds("127.0.0.1", port, api_key):
            say(f"  {OK} Reusing existing tunnel on port {port}")
            return
        # Ours, but no longer carrying traffic (laptop slept, gateway bounced).
        say(f"  {WARN} Existing tunnel on {port} is stale, replacing it")
        if not stop_tunnel(port, quiet=True):
            die(f"Could not free port {port} from the stale tunnel.\n"
                f"  Inspect it with: lsof -iTCP:{port} -sTCP:LISTEN\n"
                f"  Or use --port <PORT> to pick another.")

    if port_open("127.0.0.1", port, timeout=1.0):
        holder = port_holder(port) or "unknown process"
        die(f"Port {port} is held by something else:\n    {holder}\n"
            f"  Use --port <PORT> to pick another.")

    # ControlMaster is disabled on purpose. Sharing the multiplexed connection
    # from ~/.ssh/config means the tunnel dies whenever that master exits (e.g.
    # you close an unrelated titan shell). This mirrors the dedicated
    # *-tunnel hosts already in ~/.ssh/config.
    cmd = [
        "ssh", "-N", "-f",
        "-o", "BatchMode=yes",
        "-o", "ExitOnForwardFailure=yes",
        "-o", "ConnectTimeout=30",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "ControlMaster=no",
        "-o", "ControlPath=none",
        "-J", SSH_JUMP,
        "-L", f"127.0.0.1:{forward_spec(port)}",
        SSH_HOST,
    ]
    dest_host, dest_port = tunnel_dest()
    say(f"  Opening tunnel 127.0.0.1:{port} {DOT} {dest_host}:{dest_port} via {SSH_JUMP}")
    # `ssh -f` forks a daemon that inherits whatever stdout/stderr it is given.
    # Any pipe therefore stays open for the life of the tunnel, so a caller
    # reading that pipe (a script, a subshell, $(...)) hangs forever -- and
    # subprocess.PIPE hits exactly that trap. A real file has no reader to
    # block, so it captures ssh's diagnostics without ever holding us open.
    with tempfile.TemporaryFile(mode="w+") as errfile:
        returncode = subprocess.run(
            cmd, stdout=subprocess.DEVNULL, stderr=errfile
        ).returncode
        errfile.seek(0)
        detail = errfile.read().strip()
    if returncode != 0:
        die("SSH tunnel failed."
            + (f"\n  ssh said: {detail}" if detail else "") +
            "\n  Check your agent has the right key loaded:  ssh-add -l\n"
            f"  Then try the hop by hand:  ssh -J {SSH_JUMP} {SSH_HOST}")

    for _ in range(20):
        if port_open("127.0.0.1", port, timeout=1.0):
            break
        time.sleep(0.5)
    else:
        die(f"Tunnel on port {port} never started listening")

    if not gateway_responds("127.0.0.1", port, api_key):
        stop_tunnel(port, quiet=True)
        die(f"Tunnel opened but the gateway did not answer.\n"
            f"  Is LiteLLM still up?  ssh -J {SSH_JUMP} {SSH_HOST} "
            f"'curl -s localhost:{GATEWAY_PORT}/v1/models'")
    say(f"  {OK} Tunnel up and gateway responding")


# --- credentials ------------------------------------------------------------

def resolve_api_key():
    """Find the gateway key: env wins, then opencode.json, then the known key."""
    env = os.environ.get("SPARK_API_KEY")
    if env:
        return env.strip(), "$SPARK_API_KEY"
    try:
        with open(OPENCODE_CONFIG) as fh:
            cfg = json.load(fh)
        auth = cfg["provider"]["spark"]["options"]["headers"]["Authorization"]
        if auth.lower().startswith("bearer "):
            return auth[7:].strip(), "opencode.json"
    except Exception:
        pass
    return FALLBACK_KEY, "built-in default"


# --- client configuration ---------------------------------------------------

def update_opencode(base_url, api_key):
    try:
        with open(OPENCODE_CONFIG) as fh:
            cfg = json.load(fh)
    except FileNotFoundError:
        say(f"  {WARN} opencode config not found: {OPENCODE_CONFIG}")
        return False
    except json.JSONDecodeError as exc:
        say(f"  {WARN} Could not parse {OPENCODE_CONFIG}: {exc}")
        return False

    provider = cfg.setdefault("provider", {}).setdefault("spark", {})
    provider.setdefault("npm", "@ai-sdk/openai-compatible")
    provider.setdefault("name", "Spark (local DGX)")
    options = provider.setdefault("options", {})
    options["baseURL"] = f"{base_url}/v1"
    options.setdefault("headers", {})["Authorization"] = f"Bearer {api_key}"

    with open(OPENCODE_CONFIG, "w") as fh:
        json.dump(cfg, fh, indent=2)
        fh.write("\n")
    say(f"  {OK} opencode   provider.spark.baseURL {DOT} {base_url}/v1")
    return True


def update_codex(base_url):
    """Rewrite the managed block in codex's TOML.

    The block is always re-appended at EOF. A TOML table header captures every
    key that follows it, so anchoring at the end guarantees the block can only
    ever contain its own keys.
    """
    try:
        with open(CODEX_CONFIG) as fh:
            text = fh.read()
    except FileNotFoundError:
        say(f"  {WARN} codex config not found: {CODEX_CONFIG}")
        return False

    text = re.sub(
        re.escape(CODEX_BEGIN) + r".*?" + re.escape(CODEX_END) + r"\n?",
        "", text, flags=re.DOTALL,
    ).rstrip("\n")

    block = f"""{CODEX_BEGIN}
# Managed by spark-shim. Edits here are overwritten; change the script instead.
[model_providers.spark]
name = "Spark (DGX titan)"
base_url = "{base_url}/v1"
env_key = "SPARK_API_KEY"
wire_api = "chat"

[profiles.spark]
model_provider = "spark"
model = "{BIG_MODEL}"

[profiles.spark-fast]
model_provider = "spark"
model = "{SMALL_MODEL}"
{CODEX_END}"""

    with open(CODEX_CONFIG, "w") as fh:
        fh.write(text + "\n\n" + block + "\n")
    say(f"  {OK} codex      profile 'spark' {DOT} {base_url}/v1")
    return True


def update_claude(base_url, api_key):
    """Repoint Claude Code at the gateway, backing up the displaced config.

    Claude Code has exactly one ANTHROPIC_BASE_URL, which argo-shim also owns.
    The two cannot coexist, so the previous settings are saved for --restore-claude.
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    try:
        with open(CLAUDE_SETTINGS) as fh:
            settings = json.load(fh)
    except FileNotFoundError:
        settings = {}
    except json.JSONDecodeError as exc:
        say(f"  {WARN} Could not parse {CLAUDE_SETTINGS}: {exc}")
        return False

    env = settings.setdefault("env", {})
    previously_spark = env.get("_SPARK_SHIM_MANAGED") == "1"
    if not previously_spark and os.path.exists(CLAUDE_SETTINGS):
        # Only ever back up a config we don't already own, so running --claude
        # twice can't overwrite the Argo settings we promise to restore.
        shutil.copyfile(CLAUDE_SETTINGS, CLAUDE_BACKUP)
        say(f"  {OK} Backed up previous Claude settings {DOT} {CLAUDE_BACKUP}")

    settings["apiKeyHelper"] = f"echo {api_key}"
    env["ANTHROPIC_BASE_URL"] = base_url
    env["ANTHROPIC_MODEL"] = BIG_MODEL
    env["ANTHROPIC_SMALL_FAST_MODEL"] = SMALL_MODEL
    env["CLAUDE_CODE_SKIP_ANTHROPIC_AUTH"] = "1"
    env["_SPARK_SHIM_MANAGED"] = "1"
    for var in ("no_proxy", "NO_PROXY"):
        hosts = [h.strip() for h in env.get(var, "").split(",") if h.strip()]
        for host in ("localhost", "127.0.0.1"):
            if host not in hosts:
                hosts.append(host)
        env[var] = ",".join(hosts)

    with open(CLAUDE_SETTINGS, "w") as fh:
        json.dump(settings, fh, indent=2)
        fh.write("\n")
    say(f"  {OK} claude     ANTHROPIC_BASE_URL {DOT} {base_url}")
    say(f"  {WARN} Claude Code now talks to Spark, not Argo. "
        f"Undo with: spark-shim --restore-claude")
    return True


def restore_claude():
    if not os.path.exists(CLAUDE_BACKUP):
        die(f"No backup to restore at {CLAUDE_BACKUP}")
    shutil.copyfile(CLAUDE_BACKUP, CLAUDE_SETTINGS)
    say(f"{OK} Restored Claude Code settings from {CLAUDE_BACKUP}")
    try:
        with open(CLAUDE_SETTINGS) as fh:
            url = json.load(fh).get("env", {}).get("ANTHROPIC_BASE_URL", "?")
        say(f"  ANTHROPIC_BASE_URL {DOT} {url}")
    except Exception:
        pass


# --- reporting --------------------------------------------------------------

def fetch_models(base_url, api_key):
    req = urllib.request.Request(
        f"{base_url}/v1/models", headers={"Authorization": f"Bearer {api_key}"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return [m["id"] for m in json.loads(resp.read().decode()).get("data", [])]


def roundtrip(base_url, api_key, path, payload, headers):
    """POST `payload` and return (ok, detail) for a real inference call."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{base_url}{path}", data=body,
        headers={"content-type": "application/json", **headers},
    )
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read().decode())
        return True, f"HTTP {resp.status} in {time.time() - started:.1f}s"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}: {exc.read()[:200].decode(errors='replace')}"
    except Exception as exc:
        return False, str(exc)


def run_tests(base_url, api_key):
    say("\nRound-trip tests (real inference, may take a moment):")
    ok_openai, detail = roundtrip(
        base_url, api_key, "/v1/chat/completions",
        {"model": BIG_MODEL, "max_tokens": 16,
         "messages": [{"role": "user", "content": "say hi"}]},
        {"Authorization": f"Bearer {api_key}"},
    )
    say(f"  [1/2] OpenAI    /v1/chat/completions  "
        f"{OK if ok_openai else BAD} {detail}")

    ok_anthropic, detail = roundtrip(
        base_url, api_key, "/v1/messages",
        {"model": BIG_MODEL, "max_tokens": 16,
         "messages": [{"role": "user", "content": "say hi"}]},
        {"x-api-key": api_key, "anthropic-version": "2023-06-01"},
    )
    say(f"  [2/2] Anthropic /v1/messages          "
        f"{OK if ok_anthropic else BAD} {detail}")
    return ok_openai and ok_anthropic


def show_status(port, api_key, key_source):
    say("spark-shim status\n")
    say(f"  API key source     {key_source}")

    pids = tunnel_pids(port)
    if pids:
        say(f"  Tunnel             {OK} running on 127.0.0.1:{port} "
            f"(PID {', '.join(map(str, pids))})")
    else:
        say(f"  Tunnel             {DOT} not running on port {port}")

    relay = socat_pids(port)
    if relay:
        say(f"  Relay 0.0.0.0      {OK} running on 0.0.0.0:{port} "
            f"(PID {', '.join(map(str, relay))})")
    else:
        say(f"  Relay 0.0.0.0      {DOT} not running on port {port}")

    direct = port_open(GATEWAY_HOST, GATEWAY_PORT, timeout=3.0)
    say(f"  ALCF network       {OK + ' yes (direct path available)' if direct else DOT + ' no (off-site, tunnel required)'}")

    for label, url in (("via tunnel", f"http://127.0.0.1:{port}"),
                       ("direct", f"http://{GATEWAY_HOST}:{GATEWAY_PORT}")):
        host = GATEWAY_HOST if label == "direct" else "127.0.0.1"
        prt = GATEWAY_PORT if label == "direct" else port
        live = gateway_responds(host, prt, api_key, timeout=5)
        say(f"  Gateway {label:<10} {OK if live else DOT} {url}")

    say("\n  Client configuration:")
    try:
        with open(OPENCODE_CONFIG) as fh:
            url = json.load(fh)["provider"]["spark"]["options"]["baseURL"]
        say(f"    opencode   {url}")
    except Exception:
        say(f"    opencode   {DOT} not configured")
    try:
        with open(CODEX_CONFIG) as fh:
            text = fh.read()
        match = re.search(r'base_url = "([^"]+)"', text[text.find(CODEX_BEGIN):]) \
            if CODEX_BEGIN in text else None
        say(f"    codex      {match.group(1) if match else DOT + ' not configured'}")
    except Exception:
        say(f"    codex      {DOT} not configured")
    try:
        with open(CLAUDE_SETTINGS) as fh:
            env = json.load(fh).get("env", {})
        managed = " (spark-shim)" if env.get("_SPARK_SHIM_MANAGED") == "1" else " (not spark)"
        say(f"    claude     {env.get('ANTHROPIC_BASE_URL', '?')}{managed}")
    except Exception:
        say(f"    claude     {DOT} not configured")


# --- entry point ------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="spark-shim",
        description="Reach the DGX LiteLLM gateway on titan from anywhere.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Default action: connect (auto-detecting direct vs tunnel) and "
               "configure opencode + codex.",
    )
    parser.add_argument("--port", type=int, default=int(os.environ.get("SPARK_SHIM_PORT", DEFAULT_PORT)),
                        help=f"local port for the tunnel (default: {DEFAULT_PORT})")
    parser.add_argument("--gateway", default=None, metavar="HOST[:PORT]",
                        help="gateway endpoint override (default: titan.alcf.anl.gov:4000); "
                             "scheme optional, also settable via $SPARK_GATEWAY")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--direct", action="store_true",
                      help="force direct connection, skip the tunnel (ALCF network only)")
    mode.add_argument("--tunnel", action="store_true",
                      help="force the SSH tunnel even if titan looks directly reachable")
    mode.add_argument("--publish", action="store_true",
                      help="ALCF login node: publish a 0.0.0.0 socat relay to the gateway "
                           "and point clients at <hostname>:<port> for compute nodes")
    parser.add_argument("--claude", action="store_true",
                        help="also repoint Claude Code at Spark (displaces Argo; backed up)")
    parser.add_argument("--no-opencode", action="store_true", help="don't touch opencode.json")
    parser.add_argument("--no-codex", action="store_true", help="don't touch codex config.toml")
    parser.add_argument("--restore-claude", action="store_true",
                        help="restore the Claude Code settings displaced by --claude")
    parser.add_argument("--status", action="store_true", help="show status and exit")
    parser.add_argument("--stop", action="store_true", help="tear down the tunnel and relay, then exit")
    parser.add_argument("--models", action="store_true", help="list served models and exit")
    parser.add_argument("--test", action="store_true",
                        help="run real round-trip tests on both wire protocols")
    args = parser.parse_args()

    gateway_spec = args.gateway or os.environ.get("SPARK_GATEWAY")
    if gateway_spec:
        apply_gateway(gateway_spec)
        say(f"  {DOT} Gateway override: {GATEWAY_HOST}:{GATEWAY_PORT}")

    api_key, key_source = resolve_api_key()

    if args.restore_claude:
        restore_claude()
        return
    if args.stop:
        say("Stopping spark-shim tunnel and relay...")
        stop_tunnel(args.port)
        stop_relay(args.port)
        return
    if args.status:
        show_status(args.port, api_key, key_source)
        return

    # Decide how to reach the gateway.
    if args.publish:
        say("Publish mode: relaying the gateway for ALCF compute nodes...")
        if not detect_direct(api_key):
            die(f"{GATEWAY_HOST}:{GATEWAY_PORT} is not reachable from here.\n"
                "  --publish belongs on an ALCF login node; compute nodes only "
                "consume the configs it writes.")
        use_tunnel = False
    elif args.direct:
        use_tunnel = False
    elif args.tunnel:
        use_tunnel = True
    else:
        say("Locating the gateway...")
        direct = detect_direct(api_key)
        say(f"  {OK if direct else DOT} {GATEWAY_HOST}:{GATEWAY_PORT} "
            f"{'reachable directly (on ALCF network)' if direct else 'unreachable (off-site)'}")
        use_tunnel = not direct
        if direct:
            say(f"  {WARN} Compute nodes cannot reach titan directly; on a "
                f"login node, run: spark-shim --publish")

    if args.publish:
        start_relay(args.port, GATEWAY_HOST, GATEWAY_PORT)
        if not gateway_responds("127.0.0.1", args.port, api_key):
            stop_relay(args.port, quiet=True)
            die("Relay is up but the gateway did not answer through it.")
        hostname = socket.gethostname()
        base_url = f"http://{hostname}:{args.port}"
        say(f"  {OK} Relay published: {hostname}:{args.port} {DOT} {GATEWAY_HOST}:{GATEWAY_PORT}")
    elif use_tunnel:
        start_tunnel(args.port, api_key)
        base_url = f"http://127.0.0.1:{args.port}"
    else:
        base_url = f"http://{GATEWAY_HOST}:{GATEWAY_PORT}"
        if not gateway_responds(GATEWAY_HOST, GATEWAY_PORT, api_key):
            die(f"--direct requested but {GATEWAY_HOST}:{GATEWAY_PORT} is not answering.\n"
                f"  Drop --direct (or pass --tunnel) to hop through {SSH_JUMP}.")
        say(f"  {OK} Using direct connection, no tunnel needed")

    if args.models:
        say(f"\nModels served by {base_url}:")
        for model in fetch_models(base_url, api_key):
            say(f"  {model}")
        return

    say(f"\nConfiguring clients (key from {key_source}):")
    if not args.no_opencode:
        update_opencode(base_url, api_key)
    if not args.no_codex:
        update_codex(base_url)
    if args.claude:
        update_claude(base_url, api_key)

    if args.test:
        if not run_tests(base_url, api_key):
            die("Round-trip tests failed", code=1)

    say(f"\n{OK} Spark is reachable at {base_url}")
    say("\nNext steps:")
    say(f"    opencode   already live {DOT} pick a spark/* model")
    if not args.no_codex:
        say(f"    codex      export SPARK_API_KEY={api_key}")
        say(f"               codex --profile spark")
    if args.claude:
        say(f"    claude     already live {DOT} restore argo with --restore-claude")
    else:
        say(f"    claude     not repointed {DOT} run with --claude if you want it")
    if args.publish:
        say("\n  Relay runs in the background on this login node.")
        say("  Compute nodes need nothing but these shared-home configs.")
        say("  Stop it with: spark-shim --stop")
    elif use_tunnel:
        say(f"\n  Tunnel runs in the background. Stop it with: spark-shim --stop")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        say("\nInterrupted")
        sys.exit(130)
