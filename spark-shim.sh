#!/usr/bin/env python3
"""spark-shim — reach the DGX LiteLLM gateway on `titan` from anywhere.

Unlike argo-shim, this needs no HTTP translation proxy. The LiteLLM gateway on
titan:4000 already speaks BOTH wire protocols natively:

    /v1/chat/completions   OpenAI    -> opencode, codex
    /v1/messages           Anthropic -> claude code (streaming SSE included)

So the only real problem is reachability: `titan.alcf.anl.gov` is an
internal-only name (NXDOMAIN off-site), and the gateway binds a private
network. On the ALCF network you can hit it directly; from home you must hop
through the CELS gateway. SSH handles that fine, but opencode/hermes speak
HTTP, not SSH.

This script bridges that gap: clients always talk to a stable local address
(http://127.0.0.1:4100), and spark-shim decides what listens there.

    lab / VPN    ->  127.0.0.1:4100 ==python relay==> titan.alcf.anl.gov:4000
    home (off-site) ->  127.0.0.1:4100 ==ssh -J login-gce==>  titan:4000
    ALCF login   ->  0.0.0.0:4100 ==ssh -J logins.cels==> titan:4000  (MFA once)
    ALCF compute ->  http://<login-node>:4100  (relay via --publish)

One probe (a TCP connect to the gateway plus an authenticated /v1/models)
decides lab vs. away. Away, it also keeps a tunnel to the Hermes dashboard
on vr09 (127.0.0.1:9119); at the lab vr09 is reached without one.

ALCF compute nodes cannot reach titan at all -- and neither can ALCF login
nodes (every port to the DGX is filtered from the compute-center zone), so
--publish picks the relay that fits: plain socat where the gateway is
routable, otherwise SSH through the CELS network. The primary ssh relay
jumps to titan and lets *titan* dial the far end from its own loopback --
same topology as the off-site Mac tunnel, bound on 0.0.0.0 -- so only
titan:22 must be reachable from CELS, never the gateway port (which is
filtered from every CELS host). A homes.cels chain that dials the gateway
from CELS space is kept as fallback. Both need MFA once; ControlPersist
keeps the authenticated master alive so the token is typed once. The relay
binds 0.0.0.0 and rewrites the shared-home client configs to
<login-node>:<port>. Compute nodes need nothing installed -- they just use
the shared configs.

Usage:
    spark-shim                  # connect + configure opencode & hermes
    spark-shim --publish        # login node: relay the gateway for compute nodes
    spark-shim --gateway h:p    # target a different gateway endpoint
    spark-shim --where          # print "lab" or "away" (exit 0 / 1) and stop
    spark-shim --no-hermes      # ...but skip the Hermes provider & tunnel
    spark-shim --status         # what's running, what's configured
    spark-shim --test           # real round-trip on both protocols
    spark-shim --models         # list models the gateway is serving
    spark-shim --stop           # tear the tunnels/relays down
"""

import argparse
import copy
import getpass
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# --- topology ---------------------------------------------------------------
# `titan` and `login-gce` are ssh_config aliases, NOT hostnames. Their
# usernames and keys live in ~/.ssh/config. The `titan` alias itself has no
# ProxyJump: the off-site tunnel passes `-J login-gce` explicitly, so this
# script -- not the config -- supplies that hop. The off-site tunnel and the
# publish-mode ssh relays are the only SSH paths; direct routability uses
# plain TCP (the loopback relay or socat).
SSH_HOST = "titan"
SSH_JUMP = "login-gce"
REMOTE_BIND = "127.0.0.1"  # gateway listens on 0.0.0.0, but loopback-from-titan
# is the shortest path once we're on the box
DEFAULT_GATEWAY_HOST = "titan.alcf.anl.gov"
DEFAULT_GATEWAY_PORT = 4000
GATEWAY_HOST = DEFAULT_GATEWAY_HOST  # override with --gateway or $SPARK_GATEWAY
GATEWAY_PORT = DEFAULT_GATEWAY_PORT

# Hermes dashboard on the Windows box vr09: reachable from the lab directly,
# off-site only through the CELS jump. spark-shim owns this tunnel (away
# only); the wrapper no longer opens it.
HERMES_TUNNEL_PORT = 9119
HERMES_TUNNEL_HOST = "vr09.alcf.anl.gov"

# CELS jump for hosts that cannot route to titan (ALCF compute-center
# nodes): the same jump argo-shim uses for apps.inside.anl.gov, and the same
# one the off-site Mac tunnel uses (login-gce is an alias for logins.cels).
# logins.cels sshd may demand MFA -- ControlPersist keeps the master
# authenticated so the token is typed once. Note logins.cels can reach
# titan:22 but NOT the gateway port (titan:4000 is filtered from all CELS
# hosts), which is why the primary relay dials the far end from titan
# itself and the homes chain is only a fallback.
SSH_CELS_JUMP = "logins.cels.anl.gov"
SSH_CELS_TARGET = "homes.cels.anl.gov"

MAX_SSH_FAILURES = 3  # CSPO blocks the login node's IP after repeated auth failures

DEFAULT_PORT = 4100
FALLBACK_KEY = "sk-spark-anl-c72183474362a422"

# The default model, kept as a full entry (name/limits/reasoning variants)
# whenever discovery has to seed it into a fresh provider config. Models the
# gateway serves but the config doesn't list yet get a minimal entry instead,
# so discovery never overwrites hand-tuned settings.
DEFAULT_MODEL = "glm-5.3-flash-exl3"
DEFAULT_MODEL_ENTRY = {
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

OPENCODE_CONFIG = os.path.expanduser("~/.config/opencode/opencode.json")
HERMES_CONFIG = os.path.expanduser("~/.hermes/config.yaml")
CODEX_CONFIG = os.path.expanduser("~/.codex/config.toml")
STATE_DIR = os.path.expanduser("~/.cache/spark-shim")

CODEX_BEGIN = "# >>> spark-shim >>>"
CODEX_END = "# <<< spark-shim <<<"

HERMES_BEGIN = "# >>> spark-shim >>>"
HERMES_END = "# <<< spark-shim <<<"
HERMES_PROVIDER_NAME = "spark"

# Hidden mode: this script re-invokes itself, detached, as the lab loopback
# relay. Matched in process args by the flag plus the explicit --port.
RELAY_FLAG = "--_relay"

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


def decide_location(api_key, override=None, direct=None):
    """The one lab/away decision, made once per run.

    "lab" means the gateway is directly reachable (ALCF network or VPN) and
    the stable address is served by the loopback relay; "away" means the ssh
    tunnel through login-gce serves it. The same decision drives the Hermes
    tunnel and, via --where, the wrapper. --direct/--tunnel are manual
    overrides of the probe, for when it guesses wrong.
    """
    if override == "direct":
        return "lab"
    if override == "tunnel":
        return "away"
    if direct is None:
        direct = detect_direct(api_key)
    return "lab" if direct else "away"


def apply_gateway(spec):
    """Point GATEWAY_HOST/GATEWAY_PORT at `spec` ("host[:port]", scheme optional)."""
    global GATEWAY_HOST, GATEWAY_PORT
    spec = spec.strip()
    for scheme in ("http://", "https://"):
        if spec.startswith(scheme):
            spec = spec[len(scheme) :]
            break
    spec = spec.rstrip("/")
    host, _, port = spec.partition(":")
    if not host or (port and not port.isdigit()):
        die(f"Invalid gateway endpoint {spec!r} (expected host[:port])")
    GATEWAY_HOST = host
    GATEWAY_PORT = int(port) if port else DEFAULT_GATEWAY_PORT


# --- tunnel lifecycle -------------------------------------------------------


def tunnel_pids(port):
    """PIDs of our own ssh tunnels for `port`.

    Matched by the -L forward spec *and* the ssh_config alias, so we never kill
    an unrelated process that merely happens to hold the port.
    """
    pids = []
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except Exception:
        return pids
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
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
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
        for pid in filter(None, (p.strip() for p in out.splitlines())):
            return subprocess.run(
                ["ps", "-o", "pid=,user=,comm=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
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


# --- loopback relay (lab / Mac) ---------------------------------------------


def loopback_relay_pids(port):
    """PIDs of our detached relay processes listening on 127.0.0.1:`port`.

    The relay is this script re-invoked in its hidden --_relay mode; matching
    on that flag plus the explicit --port (word-boundary) means we never
    signal an unrelated process that merely happens to hold the port.
    """
    pids = []
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except Exception:
        return pids
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
        except Exception:
            continue
        if (
            "spark-shim" in args
            and RELAY_FLAG in args
            and re.search(rf"--port {port}(\s|$)", args)
        ):
            pids.append(int(pid))
    return pids


def stop_loopback_relay(port, quiet=False):
    """Stop our loopback relay on `port`; True once nothing is listening."""
    pids = loopback_relay_pids(port)
    if not pids:
        if not quiet:
            say(f"  No spark-shim relay on port {port}")
        return not port_open("127.0.0.1", port, timeout=1.0)
    return _terminate_listeners(pids, port, quiet, "loopback relay")


def start_loopback_relay(port, api_key):
    """Ensure our detached relay is listening on 127.0.0.1:`port`.

    The lab path for the stable address: a plain stdlib TCP forward to the
    gateway endpoint (honouring --gateway), loopback-only -- no socat, no
    ssh, no keys. Started by re-invoking this script in its hidden relay
    mode, detached from the shell so it survives logout; found, health-
    checked and stopped by its own listen spec in the process args.
    """
    if loopback_relay_pids(port):
        if gateway_responds("127.0.0.1", port, api_key):
            say(f"  {OK} Reusing existing relay on 127.0.0.1:{port}")
            return
        # Ours, but no longer carrying traffic (laptop slept, gateway bounced).
        say(f"  {WARN} Existing relay on {port} is stale, replacing it")
        if not stop_loopback_relay(port, quiet=True):
            die(
                f"Could not free port {port} from the stale relay.\n"
                f"  Inspect it with: lsof -iTCP:{port} -sTCP:LISTEN\n"
                f"  Or use --port <PORT> to pick another."
            )
    if port_open("127.0.0.1", port, timeout=1.0):
        holder = port_holder(port) or "unknown process"
        die(
            f"Port {port} is held by something else:\n    {holder}\n"
            f"  Use --port <PORT> to pick another."
        )

    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        RELAY_FLAG,
        "--port",
        str(port),
        "--gateway",
        f"{GATEWAY_HOST}:{GATEWAY_PORT}",
    ]
    say(f"  Starting relay 127.0.0.1:{port} {DOT} {GATEWAY_HOST}:{GATEWAY_PORT} (detached)")
    # Detached like the socat relay: own session, stdio to devnull, so no
    # pipe can hold a reader open and it outlives this shell.
    subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(20):
        if port_open("127.0.0.1", port, timeout=1.0):
            break
        time.sleep(0.5)
    else:
        stop_loopback_relay(port, quiet=True)
        die(f"Relay on port {port} never started listening")
    if not gateway_responds("127.0.0.1", port, api_key):
        stop_loopback_relay(port, quiet=True)
        die("Relay is up but the gateway did not answer through it.")
    say(f"  {OK} Relay up and gateway responding")


def _pipe(src, dst, finished=None):
    """Copy src -> dst until EOF, then half-close dst (and notify if given)."""
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        if finished is not None:
            finished()


def _relay_conn(client, dest_host, dest_port):
    """Bridge one accepted connection to dest_host:dest_port, both ways.

    Each direction half-closes its target when it ends; the sockets are fully
    closed only once BOTH directions are done, so a client that half-closes
    its request still receives the response.
    """
    try:
        upstream = socket.create_connection((dest_host, dest_port), timeout=15)
    except OSError:
        client.close()
        return

    lock = threading.Lock()
    open_dirs = [2]

    def _finished():
        with lock:
            open_dirs[0] -= 1
            if open_dirs[0] <= 0:
                for sock in (client, upstream):
                    try:
                        sock.close()
                    except OSError:
                        pass

    back = threading.Thread(
        target=_pipe, args=(upstream, client, _finished), daemon=True
    )
    back.start()
    _pipe(client, upstream, _finished)


def serve_relay(listen_host, listen_port, dest_host, dest_port):
    """Blocking stdlib relay: every connection forwarded to dest_host:dest_port.

    Runs only in the hidden --_relay mode, as a detached process. SIGTERM/
    SIGINT end it cleanly with exit 0 -- that's how --stop and the location
    switch-over shut it down.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((listen_host, listen_port))
    srv.listen(64)

    def _end(signum, frame):
        raise SystemExit(0)

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, _end)
        signal.signal(signal.SIGINT, _end)
    try:
        while True:
            try:
                client, _ = srv.accept()
            except OSError:
                break  # listening socket closed
            threading.Thread(
                target=_relay_conn,
                args=(client, dest_host, dest_port),
                daemon=True,
            ).start()
    finally:
        srv.close()


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
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except Exception:
        return pids
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
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
        die(
            "socat not found in PATH; publish mode needs it to expose the "
            "gateway on 0.0.0.0."
        )
    if socat_pids(port):
        say(f"  {OK} Reusing existing relay on 0.0.0.0:{port}")
        return
    if port_open("127.0.0.1", port, timeout=1.0):
        holder = port_holder(port) or "unknown process"
        die(
            f"Port {port} is held by something else:\n    {holder}\n"
            f"  Use --port <PORT> to pick another."
        )

    cmd = [
        "socat",
        f"TCP-LISTEN:{port},fork,reuseaddr,bind=0.0.0.0",
        f"TCP:{dest_host}:{dest_port}",
    ]
    say(f"  Publishing 0.0.0.0:{port} {DOT} {dest_host}:{dest_port} (socat)")
    # start_new_session detaches the relay from this shell so it survives
    # logout, the way `nohup ... &` would; stdio to devnull so no pipe can
    # hold a reader open.
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
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


def cels_username():
    return os.environ.get("CELS_USERNAME") or getpass.getuser()


def ssh_relay_pids(port):
    """PIDs of our ssh-chain relay processes for `port`."""
    pids = []
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except Exception:
        return pids
    spec = f"0.0.0.0:{port}:{GATEWAY_HOST}:{GATEWAY_PORT}"
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
        except Exception:
            continue
        if "ssh" in args and spec in args and SSH_CELS_TARGET in args:
            pids.append(int(pid))
    return pids


def stop_ssh_relay(port, quiet=False):
    """Stop our ssh-chain relay on `port`; True once nothing is listening."""
    pids = ssh_relay_pids(port)
    if not pids:
        if not quiet:
            say(f"  No spark-shim ssh relay on port {port}")
        return not port_open("127.0.0.1", port, timeout=1.0)
    return _terminate_listeners(pids, port, quiet, "ssh relay")


def titan_ssh_relay_pids(port):
    """PIDs of our titan-loopback ssh relays listening for `port`.

    Matched by the forward spec (far end = tunnel_dest, i.e. titan's
    loopback by default) plus the CELS jump and the absence of the homes
    target, so it never collides with a homes-chain relay.
    """
    pids = []
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except Exception:
        return pids
    dest_host, dest_port = tunnel_dest()
    spec = f"0.0.0.0:{port}:{dest_host}:{dest_port}"
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
        except Exception:
            continue
        if (
            "ssh" in args
            and spec in args
            and SSH_CELS_JUMP in args
            and SSH_CELS_TARGET not in args
        ):
            pids.append(int(pid))
    return pids


def stop_titan_ssh_relay(port, quiet=False):
    """Stop our titan-loopback ssh relay on `port`; True once nothing listens."""
    pids = titan_ssh_relay_pids(port)
    if not pids:
        if not quiet:
            say(f"  No spark-shim ssh relay on port {port}")
        return not port_open("127.0.0.1", port, timeout=1.0)
    return _terminate_listeners(pids, port, quiet, "ssh relay")


def _ssh_relay_options(batch):
    """Shared option block for publish-mode ssh relays (both routes)."""
    opts = [
        "-N",
        "-f",
        "-o",
        "ConnectionAttempts=1",
        "-o",
        "ConnectTimeout=20",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=4",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ControlMaster=auto",
        "-o",
        "ControlPath=~/.ssh/spark-shim-%C",
        "-o",
        "ControlPersist=yes",
    ]
    if batch:
        opts += ["-o", "BatchMode=yes"]
    return opts


def ssh_relay_cmd(port, batch):
    """homes.cels-chain relay: dial the gateway from homes (fallback route)."""
    user = cels_username()
    return (
        ["ssh"]
        + _ssh_relay_options(batch)
        + [
            "-J",
            f"{user}@{SSH_CELS_JUMP}",
            "-L",
            f"0.0.0.0:{port}:{GATEWAY_HOST}:{GATEWAY_PORT}",
            f"{user}@{SSH_CELS_TARGET}",
        ]
    )


def titan_username():
    """ssh login for the titan end of the publish relay.

    Not the CELS account: titan takes the ALCF username. Override with
    $SPARK_TITAN_USER if it ever differs from the local username.
    """
    return os.environ.get("SPARK_TITAN_USER") or getpass.getuser()


def titan_ssh_relay_cmd(port, batch):
    """titan-loopback relay (primary route): jump to titan via logins.cels and
    dial the far end from titan's own loopback.

    Only titan:22 must be reachable from the jump -- the gateway port never
    has to be routable from CELS at all. Same topology as the off-site Mac
    tunnel, bound on 0.0.0.0 instead of loopback.
    """
    return (
        ["ssh"]
        + _ssh_relay_options(batch)
        + [
            "-J",
            f"{cels_username()}@{SSH_CELS_JUMP}",
            "-L",
            f"0.0.0.0:{port}:{tunnel_dest()[0]}:{tunnel_dest()[1]}",
            f"{titan_username()}@{GATEWAY_HOST}",
        ]
    )


def ssh_fail_count():
    try:
        with open(os.path.join(STATE_DIR, "ssh-fail-count")) as fh:
            return int(fh.read().strip())
    except Exception:
        return 0


def record_ssh_result(ok):
    os.makedirs(STATE_DIR, exist_ok=True)
    count = 0 if ok else ssh_fail_count() + 1
    try:
        with open(os.path.join(STATE_DIR, "ssh-fail-count"), "w") as fh:
            fh.write(str(count))
    except Exception:
        pass


def start_ssh_relay(port):
    """Ensure a homes-chain ssh relay 0.0.0.0:{port} -> gateway via the CELS pair.

    Fallback publish route (used if the titan-loopback relay doesn't answer):
    the forward's far end is dialed from homes.cels, which needs homes.cels
    to reach the gateway port -- a route that has proven fragile. homes.cels
    requires MFA, so with a TTY the command runs without BatchMode and the
    prompt appears inline; ControlPersist keeps the master authenticated for
    all future runs, including non-interactive ones.
    """
    if ssh_relay_pids(port):
        say(f"  {OK} Reusing existing ssh relay on 0.0.0.0:{port}")
        return
    if port_open("127.0.0.1", port, timeout=1.0):
        holder = port_holder(port) or "unknown process"
        die(
            f"Port {port} is held by something else:\n    {holder}\n"
            f"  Use --port <PORT> to pick another."
        )

    fails = ssh_fail_count()
    if fails >= MAX_SSH_FAILURES:
        die(
            f"SSH to CELS has failed {fails} consecutive times; not retrying "
            f"to avoid getting this login node's IP blocked.\n"
            f"  Bootstrap once interactively (answer the MFA prompt), then "
            f"delete: {STATE_DIR}/ssh-fail-count"
        )

    interactive = sys.stdin.isatty()
    cmd = ssh_relay_cmd(port, batch=not interactive)
    # ssh -f forks a daemon that inherits stdout/stderr, so a captured pipe
    # would stay open for the relay's lifetime and hang the caller -- stderr
    # goes to a real file instead (same trap start_tunnel documents).
    if interactive:
        say("  Opening ssh relay via CELS (answer the MFA prompt if shown)")
        with tempfile.TemporaryFile(mode="w+") as errfile:
            code = subprocess.run(
                cmd, stdout=subprocess.DEVNULL, stderr=errfile
            ).returncode
            if code != 0:
                errfile.seek(0)
                detail = errfile.read().strip()
                say(f"  ssh said: {detail}")
    else:
        say("  Opening ssh relay via CELS (non-interactive, BatchMode)")
        with tempfile.TemporaryFile(mode="w+") as errfile:
            code = subprocess.run(
                cmd, stdout=subprocess.DEVNULL, stderr=errfile
            ).returncode
            errfile.seek(0)
            detail = errfile.read().strip()
        if code != 0:
            say(
                "  BatchMode could not authenticate: no MFA master is "
                "available in a non-interactive shell"
                + (f" -- ssh said: {detail}" if detail else "")
            )
    if code != 0:
        record_ssh_result(False)
        die(
            "ssh relay failed.\n"
            "  Run this once from an interactive shell on this node and "
            "answer the MFA prompt:\n"
            f"    {' '.join(ssh_relay_cmd(port, batch=False))}\n"
            "  The ControlPersist master then stays authenticated, and "
            "--publish works non-interactively afterwards."
        )

    for _ in range(20):
        if port_open("127.0.0.1", port, timeout=1.0):
            break
        time.sleep(0.5)
    else:
        record_ssh_result(False)
        stop_ssh_relay(port, quiet=True)
        die(f"ssh relay on port {port} never started listening")
    record_ssh_result(True)
    say(f"  {OK} ssh relay published on 0.0.0.0:{port} (via {SSH_CELS_TARGET})")


def start_titan_ssh_relay(port):
    """Ensure a titan-loopback ssh relay 0.0.0.0:{port} -> gateway.

    Primary publish route on ALCF compute-center nodes: jump through the
    CELS login gateway to titan and dial the forward's far end from titan's
    own loopback (same topology as the off-site Mac tunnel, bound on
    0.0.0.0). Only titan:22 must be reachable from CELS -- the gateway port
    itself is filtered from every CELS host, which is what makes the
    homes.cels chain unreliable. Returns True once the port listens; False
    on any failure so the caller can fall back to the homes chain.
    """
    if titan_ssh_relay_pids(port):
        say(f"  {OK} Reusing existing ssh relay on 0.0.0.0:{port}")
        return True
    if port_open("127.0.0.1", port, timeout=1.0):
        holder = port_holder(port) or "unknown process"
        die(
            f"Port {port} is held by something else:\n    {holder}\n"
            f"  Use --port <PORT> to pick another."
        )

    fails = ssh_fail_count()
    if fails >= MAX_SSH_FAILURES:
        say(
            f"  {WARN} SSH to CELS has failed {fails} consecutive times; "
            f"not retrying to avoid getting this login node's IP blocked.\n"
            f"  Bootstrap once interactively (answer the MFA prompt), then "
            f"delete: {STATE_DIR}/ssh-fail-count"
        )
        return False

    interactive = sys.stdin.isatty()
    cmd = titan_ssh_relay_cmd(port, batch=not interactive)
    # ssh -f forks a daemon that inherits stdout/stderr, so a captured pipe
    # would stay open for the relay's lifetime and hang the caller -- stderr
    # goes to a real file instead (same trap start_tunnel documents).
    if interactive:
        say("  Opening ssh relay to titan via CELS (answer the MFA prompt if shown)")
        with tempfile.TemporaryFile(mode="w+") as errfile:
            code = subprocess.run(
                cmd, stdout=subprocess.DEVNULL, stderr=errfile
            ).returncode
            if code != 0:
                errfile.seek(0)
                detail = errfile.read().strip()
                say(f"  ssh said: {detail}")
    else:
        say("  Opening ssh relay to titan via CELS (non-interactive, BatchMode)")
        with tempfile.TemporaryFile(mode="w+") as errfile:
            code = subprocess.run(
                cmd, stdout=subprocess.DEVNULL, stderr=errfile
            ).returncode
            errfile.seek(0)
            detail = errfile.read().strip()
        if code != 0:
            say(
                "  BatchMode could not authenticate: no MFA master is "
                "available in a non-interactive shell"
                + (f" -- ssh said: {detail}" if detail else "")
            )
    if code != 0:
        record_ssh_result(False)
        say(
            f"  {BAD} ssh relay to titan failed. To bootstrap by hand, run "
            f"this once from an interactive shell on this node and answer "
            f"the MFA prompt:\n"
            f"    {' '.join(titan_ssh_relay_cmd(port, batch=False))}"
        )
        return False

    for _ in range(20):
        if port_open("127.0.0.1", port, timeout=1.0):
            break
        time.sleep(0.5)
    else:
        record_ssh_result(False)
        stop_titan_ssh_relay(port, quiet=True)
        say(f"  {BAD} ssh relay on port {port} never started listening")
        return False
    record_ssh_result(True)
    say(f"  {OK} ssh relay published on 0.0.0.0:{port} (far end dialed by titan)")
    return True


def start_tunnel(port, api_key):
    """Ensure a working tunnel on `port`; reuse a healthy one if present."""
    if tunnel_pids(port):
        if gateway_responds("127.0.0.1", port, api_key):
            say(f"  {OK} Reusing existing tunnel on port {port}")
            return
        # Ours, but no longer carrying traffic (laptop slept, gateway bounced).
        say(f"  {WARN} Existing tunnel on {port} is stale, replacing it")
        if not stop_tunnel(port, quiet=True):
            die(
                f"Could not free port {port} from the stale tunnel.\n"
                f"  Inspect it with: lsof -iTCP:{port} -sTCP:LISTEN\n"
                f"  Or use --port <PORT> to pick another."
            )

    if port_open("127.0.0.1", port, timeout=1.0):
        holder = port_holder(port) or "unknown process"
        die(
            f"Port {port} is held by something else:\n    {holder}\n"
            f"  Use --port <PORT> to pick another."
        )

    # ControlMaster is disabled on purpose. Sharing the multiplexed connection
    # from ~/.ssh/config means the tunnel dies whenever that master exits (e.g.
    # you close an unrelated titan shell). This mirrors the dedicated
    # *-tunnel hosts already in ~/.ssh/config.
    cmd = [
        "ssh",
        "-N",
        "-f",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ConnectTimeout=30",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ControlMaster=no",
        "-o",
        "ControlPath=none",
        "-J",
        SSH_JUMP,
        "-L",
        f"127.0.0.1:{forward_spec(port)}",
        SSH_HOST,
    ]
    dest_host, dest_port = tunnel_dest()
    say(
        f"  Opening tunnel 127.0.0.1:{port} {DOT} {dest_host}:{dest_port} via {SSH_JUMP}"
    )
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
        die(
            "SSH tunnel failed."
            + (f"\n  ssh said: {detail}" if detail else "")
            + "\n  Check your agent has the right key loaded:  ssh-add -l\n"
            f"  Then try the hop by hand:  ssh -J {SSH_JUMP} {SSH_HOST}"
        )

    for _ in range(20):
        if port_open("127.0.0.1", port, timeout=1.0):
            break
        time.sleep(0.5)
    else:
        die(f"Tunnel on port {port} never started listening")

    if not gateway_responds("127.0.0.1", port, api_key):
        stop_tunnel(port, quiet=True)
        die(
            f"Tunnel opened but the gateway did not answer.\n"
            f"  Is LiteLLM still up?  ssh -J {SSH_JUMP} {SSH_HOST} "
            f"'curl -s localhost:{GATEWAY_PORT}/v1/models'"
        )
    say(f"  {OK} Tunnel up and gateway responding")


# --- Hermes dashboard tunnel (away only) ------------------------------------


def hermes_tunnel_pids():
    """PIDs of our own ssh forwards for the Hermes dashboard.

    Matched by the -L forward spec *and* the jump alias, like tunnel_pids.
    """
    pids = []
    spec = f"127.0.0.1:{HERMES_TUNNEL_PORT}:{HERMES_TUNNEL_HOST}:{HERMES_TUNNEL_PORT}"
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"TCP:{HERMES_TUNNEL_PORT}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
    except Exception:
        return pids
    for pid in filter(None, (p.strip() for p in out.splitlines())):
        try:
            args = subprocess.run(
                ["ps", "-o", "args=", "-p", pid],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout
        except Exception:
            continue
        if "ssh" in args and spec in args and SSH_JUMP in args:
            pids.append(int(pid))
    return pids


def hermes_tunnel_healthy():
    """True if the Hermes forward actually carries traffic.

    Judged by making an HTTP request through the forward: ANY response
    counts (the dashboard may 401 or redirect), while a refusal, a close
    with no data, or a timeout means the tunnel is a hung leftover --
    exactly what a laptop sleep leaves behind. A bare TCP connect is NOT
    enough: ssh's local listener accepts connections even when the far end
    is unreachable, which would report a dead forward as healthy.
    """
    try:
        with socket.create_connection(
            ("127.0.0.1", HERMES_TUNNEL_PORT), timeout=3
        ) as s:
            s.sendall(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
            return bool(s.recv(256))
    except OSError:
        return False


def hermes_tunnel_cmd():
    """The forward itself: 127.0.0.1:9119 -> vr09:9119 via login-gce.

    A dedicated non-multiplexed connection, like the Spark tunnel -- sharing
    the login-gce master would kill the dashboard whenever that master exits.
    """
    return [
        "ssh",
        "-N",
        "-f",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ConnectTimeout=30",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ControlMaster=no",
        "-o",
        "ControlPath=none",
        "-L",
        f"127.0.0.1:{HERMES_TUNNEL_PORT}:{HERMES_TUNNEL_HOST}:{HERMES_TUNNEL_PORT}",
        SSH_JUMP,
    ]


def stop_hermes_tunnel(quiet=False):
    """Stop our Hermes forward; True once nothing is listening on 9119."""
    pids = hermes_tunnel_pids()
    if not pids:
        if not quiet:
            say(f"  No spark-shim Hermes tunnel on port {HERMES_TUNNEL_PORT}")
        return not port_open("127.0.0.1", HERMES_TUNNEL_PORT, timeout=1.0)
    return _terminate_listeners(pids, HERMES_TUNNEL_PORT, quiet, "Hermes tunnel")


def ensure_hermes_tunnel(location):
    """Keep a working Hermes dashboard forward when away; nothing at the lab.

    At the lab vr09 is reached directly, so nothing is started AND nothing
    is killed -- the login-gce master may be serving other sessions.
    Away, a listening-but-dead forward owned by ssh is detected via the
    health check and replaced; a foreign holder of the port is left alone.
    """
    if location != "away":
        say(
            f"  {DOT} Hermes dashboard reachable directly at the lab "
            f"(no tunnel on port {HERMES_TUNNEL_PORT})"
        )
        return
    pids = hermes_tunnel_pids()
    if pids:
        if hermes_tunnel_healthy():
            say(f"  {OK} Reusing existing Hermes tunnel on port {HERMES_TUNNEL_PORT}")
            return
        say(f"  {WARN} Hermes tunnel on {HERMES_TUNNEL_PORT} is dead, replacing it")
        if not stop_hermes_tunnel(quiet=True):
            say(
                f"  {WARN} Could not free port {HERMES_TUNNEL_PORT}; leaving the "
                f"dead Hermes tunnel in place"
            )
            return
    elif port_open("127.0.0.1", HERMES_TUNNEL_PORT, timeout=1.0):
        holder = port_holder(HERMES_TUNNEL_PORT) or "unknown process"
        say(
            f"  {WARN} Port {HERMES_TUNNEL_PORT} is held by something else, "
            f"leaving it alone:\n    {holder}"
        )
        return

    with tempfile.TemporaryFile(mode="w+") as errfile:
        returncode = subprocess.run(
            hermes_tunnel_cmd(), stdout=subprocess.DEVNULL, stderr=errfile
        ).returncode
        errfile.seek(0)
        detail = errfile.read().strip()
    if returncode != 0:
        say(
            f"  {WARN} Hermes tunnel failed to open."
            + (f"\n  ssh said: {detail}" if detail else "")
        )
        return
    say(f"  {OK} Hermes tunnel up: 127.0.0.1:{HERMES_TUNNEL_PORT} {DOT} {HERMES_TUNNEL_HOST}:{HERMES_TUNNEL_PORT}")


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


def merge_models(existing, discovered):
    """Union of the config's models and the gateway's, preserving tuning.

    Existing entries (names, limits, reasoning variants) are kept verbatim;
    models the gateway serves but the config lacks get minimal entries. The
    default model is seeded with its full entry so a fresh config still gets
    its reasoning variants.
    """
    models = dict(existing)
    for model_id in discovered:
        if model_id not in models:
            if model_id == DEFAULT_MODEL:
                models[model_id] = copy.deepcopy(DEFAULT_MODEL_ENTRY)
            else:
                models[model_id] = {"name": model_id}
    return models


def update_opencode(base_url, api_key, discovered=()):
    """Point opencode's spark provider at base_url and merge in discovered models.

    Returns the merged model map (for the Hermes block's context lengths), or
    None if the config doesn't exist.
    """
    try:
        with open(OPENCODE_CONFIG) as fh:
            cfg = json.load(fh)
    except FileNotFoundError:
        say(f"  {WARN} opencode config not found: {OPENCODE_CONFIG}")
        return None
    except json.JSONDecodeError as exc:
        say(f"  {WARN} Could not parse {OPENCODE_CONFIG}: {exc}")
        return None

    provider = cfg.setdefault("provider", {}).setdefault("spark", {})
    provider.setdefault("npm", "@ai-sdk/openai-compatible")
    provider.setdefault("name", "Spark (local DGX)")
    options = provider.setdefault("options", {})
    options["baseURL"] = f"{base_url}/v1"
    options.setdefault("headers", {})["Authorization"] = f"Bearer {api_key}"

    merged = merge_models(provider.get("models", {}), discovered)
    provider["models"] = merged

    with open(OPENCODE_CONFIG, "w") as fh:
        json.dump(cfg, fh, indent=2)
        fh.write("\n")
    say(f"  {OK} opencode   provider.spark.baseURL {DOT} {base_url}/v1")
    return merged


def strip_managed_block(text, begin, end):
    """Remove the marker-delimited block `begin`…`end` from `text`, if present.

    [ \\t]* on both ends: the markers may be indented, and the indentation
    must go with them or the surrounding lines shift.
    """
    return re.sub(
        r"[ \t]*" + re.escape(begin) + r".*?" + re.escape(end) + r"[ \t]*\n?",
        "",
        text,
        flags=re.DOTALL,
    )


def timestamped_backup(path, dest_dir=None):
    """Copy `path` to a timestamped backup; never overwrite an earlier one.

    Backs up next to the file by default (the convention Hermes/argo already
    use there), or into dest_dir for state-dir keepsakes.
    """
    ts = time.strftime("%Y%m%d-%H%M%S")
    base = os.path.basename(path)
    dest_dir = dest_dir or os.path.dirname(path)
    os.makedirs(dest_dir, exist_ok=True)
    candidate = os.path.join(dest_dir, f"{base}.bak-spark-shim-{ts}")
    n = 1
    while os.path.exists(candidate):
        candidate = os.path.join(dest_dir, f"{base}.bak-spark-shim-{ts}-{n}")
        n += 1
    shutil.copyfile(path, candidate)
    return candidate


def strip_codex_block():
    """One-time cleanup: remove spark-shim's old managed block from Codex TOML.

    spark-shim no longer configures Codex (Argo owns it), but the old block
    would otherwise linger forever. Every configure run strips it if present,
    with a backup; a no-op once it's gone.
    """
    try:
        with open(CODEX_CONFIG) as fh:
            text = fh.read()
    except FileNotFoundError:
        return
    except OSError:
        return
    if CODEX_BEGIN not in text:
        return
    cleaned = strip_managed_block(text, CODEX_BEGIN, CODEX_END).rstrip("\n") + "\n"
    backup = timestamped_backup(CODEX_CONFIG, dest_dir=STATE_DIR)
    with open(CODEX_CONFIG, "w") as fh:
        fh.write(cleaned)
    say(f"  {OK} codex      old spark-shim block removed (backup: {backup})")


def build_hermes_block(base_url, api_key, model_contexts, indent):
    """The managed custom_providers list item, as YAML text at `indent`."""
    pad = " " * indent
    lines = [
        f"{pad}{HERMES_BEGIN}",
        f"{pad}# Managed by spark-shim. Edits here are overwritten; change the script instead.",
        f"{pad}- name: {HERMES_PROVIDER_NAME}",
        f"{pad}  base_url: {base_url}/v1",
        f"{pad}  api_key: {api_key}",
        f"{pad}  api_mode: chat_completions",
    ]
    if model_contexts:
        lines.append(f"{pad}  models:")
        for model_id, ctx in model_contexts.items():
            if ctx:
                lines.append(f"{pad}    {model_id}:")
                lines.append(f"{pad}      context_length: {ctx}")
            else:
                # No context length known: an explicit empty mapping, so the
                # entry is a dict and not a YAML null.
                lines.append(f"{pad}    {model_id}: {{}}")
    lines.append(f"{pad}{HERMES_END}")
    return "\n".join(lines)


def update_hermes(base_url, api_key, opencode_models):
    """Write the managed Spark provider block into Hermes config.yaml.

    Plain text editing so the rest of the file (comments, ordering, Hermes's
    own model settings) survives byte for byte: the block becomes the FIRST
    item of the custom_providers list -- appending the whole section if
    Hermes never had one -- and re-runs replace it in place. A timestamped
    backup is written before any change, and nothing is written at all when
    the result would be identical.
    """
    try:
        with open(HERMES_CONFIG) as fh:
            text = fh.read()
    except FileNotFoundError:
        say(f"  {WARN} hermes config not found: {HERMES_CONFIG}")
        return False
    except OSError as exc:
        say(f"  {WARN} Could not read {HERMES_CONFIG}: {exc}")
        return False

    model_contexts = {}
    for model_id, entry in (opencode_models or {}).items():
        ctx = (entry or {}).get("limit", {}).get("context")
        model_contexts[model_id] = ctx

    # Strip any block we manage already, wherever it currently sits.
    without = strip_managed_block(text, HERMES_BEGIN, HERMES_END)

    section = re.compile(r"^custom_providers:[ \t]*(?:#.*)?$", re.MULTILINE)
    match = section.search(without)
    if match:
        # Match the indentation of the list's existing items so our block
        # reads like the rest of the file (defaults to 2 for an empty list).
        indent = 2
        for line in without[match.end() :].splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("- ") and line[0] in (" ", "-"):
                indent = len(line) - len(line.lstrip(" "))
            break
        line_end = without.find("\n", match.start())
        block = build_hermes_block(base_url, api_key, model_contexts, indent)
        if line_end == -1:
            new_text = without + "\n" + block + "\n"
        else:
            new_text = without[: line_end + 1] + block + "\n" + without[line_end + 1 :]
    else:
        # No custom_providers section at all: append one.
        block = build_hermes_block(base_url, api_key, model_contexts, 2)
        new_text = (
            without.rstrip("\n") + "\n\ncustom_providers:\n" + block + "\n"
        )

    if new_text == text:
        say(f"  {OK} hermes     provider '{HERMES_PROVIDER_NAME}' already up to date")
        return True

    backup = timestamped_backup(HERMES_CONFIG)
    with open(HERMES_CONFIG, "w") as fh:
        fh.write(new_text)
    say(
        f"  {OK} hermes     provider '{HERMES_PROVIDER_NAME}' {DOT} {base_url}/v1"
        f" (backup: {backup})"
    )
    return True


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
        f"{base_url}{path}",
        data=body,
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
        base_url,
        api_key,
        "/v1/chat/completions",
        {
            "model": DEFAULT_MODEL,
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "say hi"}],
        },
        {"Authorization": f"Bearer {api_key}"},
    )
    say(f"  [1/2] OpenAI    /v1/chat/completions  {OK if ok_openai else BAD} {detail}")

    ok_anthropic, detail = roundtrip(
        base_url,
        api_key,
        "/v1/messages",
        {
            "model": DEFAULT_MODEL,
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "say hi"}],
        },
        {"x-api-key": api_key, "anthropic-version": "2023-06-01"},
    )
    say(
        f"  [2/2] Anthropic /v1/messages          "
        f"{OK if ok_anthropic else BAD} {detail}"
    )
    return ok_openai and ok_anthropic


def _configured_urls():
    """(client, url) pairs as currently written into the client configs."""
    urls = []
    try:
        with open(OPENCODE_CONFIG) as fh:
            url = json.load(fh)["provider"]["spark"]["options"]["baseURL"]
        urls.append(("opencode", url))
    except Exception:
        urls.append(("opencode", None))
    try:
        with open(HERMES_CONFIG) as fh:
            text = fh.read()
        match = re.search(
            r"^\s*- name: " + HERMES_PROVIDER_NAME + r"\s*$"
            r".*?^\s*base_url: (\S+)",
            text,
            re.MULTILINE | re.DOTALL,
        )
        urls.append(("hermes", match.group(1) if match else None))
    except Exception:
        urls.append(("hermes", None))
    return urls


def show_status(port, api_key, key_source):
    say("spark-shim status\n")
    say(f"  API key source     {key_source}")

    direct = detect_direct(api_key)
    location = "lab" if direct else "away"
    say(
        f"  Location           {location} "
        f"({'gateway directly reachable' if direct else 'off-site, tunnel path'})"
    )

    relay = loopback_relay_pids(port)
    if relay:
        say(
            f"  Loopback relay     {OK} running on 127.0.0.1:{port} "
            f"(PID {', '.join(map(str, relay))})"
        )
    else:
        say(f"  Loopback relay     {DOT} not running on port {port}")

    tunnels = tunnel_pids(port)
    if tunnels:
        say(
            f"  Tunnel             {OK} running on 127.0.0.1:{port} "
            f"(PID {', '.join(map(str, tunnels))})"
        )
    else:
        say(f"  Tunnel             {DOT} not running on port {port}")

    if not relay and not tunnels and port_open("127.0.0.1", port, timeout=1.0):
        say(f"  {WARN} Port {port} is held by something spark-shim doesn't own")
    if location == "lab" and tunnels:
        say(f"  {WARN} Away-style tunnel running, but this looks like the lab")
    if location == "away" and relay:
        say(f"  {WARN} Lab-style relay running, but this looks like away")

    hermes = hermes_tunnel_pids()
    if hermes:
        healthy = hermes_tunnel_healthy()
        state = OK if healthy else f"{WARN} listening but not carrying traffic"
        say(
            f"  Hermes tunnel      {state} 127.0.0.1:{HERMES_TUNNEL_PORT} {DOT} "
            f"{HERMES_TUNNEL_HOST}:{HERMES_TUNNEL_PORT} via {SSH_JUMP} "
            f"(PID {', '.join(map(str, hermes))})"
        )
    else:
        say(f"  Hermes tunnel      {DOT} not running")
        if location == "away":
            say(f"  {WARN} Away, but no Hermes tunnel is up")

    socat = socat_pids(port)
    if socat:
        say(
            f"  Relay 0.0.0.0      {OK} running on 0.0.0.0:{port} "
            f"(PID {', '.join(map(str, socat))})"
        )
    else:
        say(f"  Relay 0.0.0.0      {DOT} not running on port {port}")

    ssh_relay = ssh_relay_pids(port)
    if ssh_relay:
        say(
            f"  Relay (homes ssh)  {OK} running on 0.0.0.0:{port} "
            f"(PID {', '.join(map(str, ssh_relay))})"
        )
    else:
        say(f"  Relay (homes ssh)  {DOT} not running on port {port}")

    titan_relay = titan_ssh_relay_pids(port)
    if titan_relay:
        say(
            f"  Relay (titan ssh)  {OK} running on 0.0.0.0:{port} "
            f"(PID {', '.join(map(str, titan_relay))})"
        )
    else:
        say(f"  Relay (titan ssh)  {DOT} not running on port {port}")

    say(
        f"  Direct gateway     {OK if direct else DOT} "
        f"http://{GATEWAY_HOST}:{GATEWAY_PORT}"
    )
    live = gateway_responds("127.0.0.1", port, api_key, timeout=5)
    say(f"  Gateway via shim   {OK if live else DOT} http://127.0.0.1:{port}")

    say("\n  Client configuration:")
    for client, url in _configured_urls():
        if not url:
            say(f"    {client:<10} {DOT} not configured")
            continue
        note = ""
        parsed = urllib.parse.urlparse(url)
        if parsed.hostname in ("127.0.0.1", "localhost"):
            lport = parsed.port or 80
            if not port_open("127.0.0.1", lport, timeout=1.0):
                note = f"  {WARN} nothing is listening on port {lport}"
        say(f"    {client:<10} {url}{note}")


# --- entry point ------------------------------------------------------------


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="spark-shim",
        description="Reach the DGX LiteLLM gateway on titan from anywhere.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Default action: decide lab vs. away, serve 127.0.0.1:4100 with "
        "the matching path (relay or tunnel) and configure opencode + hermes.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("SPARK_SHIM_PORT", DEFAULT_PORT)),
        help=f"local port for the stable address (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--gateway",
        default=None,
        metavar="HOST[:PORT]",
        help="gateway endpoint override (default: titan.alcf.anl.gov:4000); "
        "scheme optional, also settable via $SPARK_GATEWAY",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--direct",
        action="store_true",
        help="force the lab path (loopback relay to the gateway), even away",
    )
    mode.add_argument(
        "--tunnel",
        action="store_true",
        help="force the away path (ssh tunnel) even if titan looks reachable",
    )
    mode.add_argument(
        "--publish",
        action="store_true",
        help="ALCF login node: publish a 0.0.0.0 relay to the gateway "
        "(socat if routable, ssh via CELS otherwise) and point clients at "
        "<hostname>:<port> for compute nodes",
    )
    parser.add_argument(
        "--no-opencode", action="store_true", help="don't touch opencode.json"
    )
    parser.add_argument(
        "--no-hermes",
        action="store_true",
        help="don't touch the Hermes config or its dashboard tunnel",
    )
    parser.add_argument(
        "--where",
        action="store_true",
        help="print 'lab' or 'away' and exit (0 = lab, 1 = away)",
    )
    parser.add_argument("--status", action="store_true", help="show status and exit")
    parser.add_argument(
        "--stop", action="store_true", help="tear down every tunnel/relay we own"
    )
    parser.add_argument(
        "--models", action="store_true", help="list served models and exit"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="run real round-trip tests on both wire protocols",
    )
    parser.add_argument(
        RELAY_FLAG,
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)

    # --direct/--tunnel are manual overrides of the lab/away probe.
    override = "direct" if args.direct else ("tunnel" if args.tunnel else None)

    gateway_spec = args.gateway or os.environ.get("SPARK_GATEWAY")
    if gateway_spec:
        apply_gateway(gateway_spec)
        if not args._relay:
            say(f"  {DOT} Gateway override: {GATEWAY_HOST}:{GATEWAY_PORT}")

    # Hidden relay mode: block forever forwarding to the gateway endpoint.
    # No key, no config writes -- this process IS the stable address.
    if args._relay:
        serve_relay("127.0.0.1", args.port, GATEWAY_HOST, GATEWAY_PORT)
        return

    api_key, key_source = resolve_api_key()

    if args.where:
        location = decide_location(api_key, override)
        say(location)
        sys.exit(0 if location == "lab" else 1)

    if args.stop:
        say("Stopping spark-shim tunnels and relays...")
        stop_tunnel(args.port)
        stop_loopback_relay(args.port)
        stop_relay(args.port)
        stop_ssh_relay(args.port)
        stop_titan_ssh_relay(args.port)
        stop_hermes_tunnel()
        return
    if args.status:
        show_status(args.port, api_key, key_source)
        return

    # Decide how to reach the gateway -- one decision drives everything.
    if args.publish:
        say("Publish mode: relaying the gateway for ALCF compute nodes...")
        location = "publish"
        direct = detect_direct(api_key)
    else:
        if override is None:
            say("Locating the gateway...")
            direct = detect_direct(api_key)
            say(
                f"  {OK if direct else DOT} {GATEWAY_HOST}:{GATEWAY_PORT} "
                f"{'reachable directly (lab path: loopback relay)' if direct else 'unreachable (away path: ssh tunnel)'}"
            )
        else:
            direct = override == "direct"
        location = decide_location(api_key, override, direct)

    if args.publish:
        if direct:
            start_relay(args.port, GATEWAY_HOST, GATEWAY_PORT)
        else:
            say(
                f"  {DOT} {GATEWAY_HOST}:{GATEWAY_PORT} is not routable from here; "
                "relaying over SSH (titan loopback first, homes.cels chain as fallback)"
            )
            if not start_titan_ssh_relay(args.port):
                stop_titan_ssh_relay(args.port, quiet=True)
                say(f"  {DOT} Falling back to the homes.cels chain")
                start_ssh_relay(args.port)
        if not gateway_responds("127.0.0.1", args.port, api_key):
            stop_relay(args.port, quiet=True)
            stop_ssh_relay(args.port, quiet=True)
            stop_titan_ssh_relay(args.port, quiet=True)
            die("Relay is up but the gateway did not answer through it.")
        hostname = socket.gethostname()
        base_url = f"http://{hostname}:{args.port}"
        say(
            f"  {OK} Relay published: {hostname}:{args.port} {DOT} {GATEWAY_HOST}:{GATEWAY_PORT}"
        )
    elif location == "lab":
        # Switch-over: a leftover tunnel from an away run must not keep the
        # stable address. Only ever kill processes we own.
        if tunnel_pids(args.port):
            say(f"  {WARN} Replacing the leftover ssh tunnel with the relay")
            if not stop_tunnel(args.port, quiet=True):
                die(
                    f"Could not free port {args.port} from the stale tunnel.\n"
                    f"  Inspect it with: lsof -iTCP:{args.port} -sTCP:LISTEN"
                )
        start_loopback_relay(args.port, api_key)
        base_url = f"http://127.0.0.1:{args.port}"
    else:
        # ...and a leftover relay from a lab run must not keep it either.
        if loopback_relay_pids(args.port):
            say(f"  {WARN} Replacing the leftover loopback relay with the tunnel")
            if not stop_loopback_relay(args.port, quiet=True):
                die(
                    f"Could not free port {args.port} from the stale relay.\n"
                    f"  Inspect it with: lsof -iTCP:{args.port} -sTCP:LISTEN"
                )
        start_tunnel(args.port, api_key)
        base_url = f"http://127.0.0.1:{args.port}"

    # Hermes dashboard tunnel: away only (spark-shim owns it; the wrapper
    # no longer opens a bare ssh -L). At the lab nothing is touched.
    if location != "publish" and not args.no_hermes:
        ensure_hermes_tunnel(location)

    if args.models:
        say(f"\nModels served by {base_url}:")
        for model in fetch_models(base_url, api_key):
            say(f"  {model}")
        return

    # Model discovery: everything the gateway serves lands in the opencode
    # provider (existing entries preserved); Hermes reuses the same list.
    discovered = []
    try:
        discovered = fetch_models(base_url, api_key)
    except Exception as exc:
        say(f"  {WARN} Model discovery failed, keeping existing entries: {exc}")

    say(f"\nConfiguring clients (key from {key_source}):")
    merged = None
    if not args.no_opencode:
        merged = update_opencode(base_url, api_key, discovered)
    if not args.publish and not args.no_hermes:
        if merged is None:
            # --no-opencode (or no config): merge against what's on disk.
            try:
                with open(OPENCODE_CONFIG) as fh:
                    existing = json.load(fh)["provider"]["spark"].get("models", {})
            except Exception:
                existing = {}
            merged = merge_models(existing, discovered)
        update_hermes(base_url, api_key, merged)
    strip_codex_block()

    if args.test:
        if not run_tests(base_url, api_key):
            die("Round-trip tests failed", code=1)

    say(f"\n{OK} Spark is reachable at {base_url}")
    say("\nRestart OpenCode and Hermes so they pick up the rewritten config.")
    say("\nNext steps:")
    say(f"    opencode   already live {DOT} pick a spark/* model")
    if not args.publish and not args.no_hermes:
        say(f"    hermes     already live {DOT} provider '{HERMES_PROVIDER_NAME}'")
    if args.publish:
        say("\n  Relay runs in the background on this login node.")
        say("  Compute nodes need nothing but these shared-home configs.")
        say("  Stop it with: spark-shim --stop")
    else:
        say(f"\n  The stable address is served in the background. Stop: spark-shim --stop")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        say("\nInterrupted")
        sys.exit(130)
