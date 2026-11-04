# spark-shim

Reach the DGX LiteLLM gateway on `titan` from anywhere.

The gateway on `titan:4000` already speaks both wire protocols natively —
`/v1/chat/completions` (OpenAI) and `/v1/messages` (Anthropic, streaming SSE
included) — so unlike argo-shim there is no HTTP translation proxy here. The
only real problem is reachability, and this script bridges it: clients always
talk to one stable local address, `http://127.0.0.1:4100`, and spark-shim
decides what listens there.

## Topology

    lab / VPN    ->  127.0.0.1:4100 ==python relay==> titan.alcf.anl.gov:4000
    home (off-site) ->  127.0.0.1:4100 ==ssh -J login-gce==>  titan:4000
    ALCF login   ->  0.0.0.0:4100 ==ssh -J logins.cels==> titan:4000  (MFA once)
    ALCF compute ->  http://<login-node>:4100  (relay via --publish)

One probe — a 3-second TCP connect to the gateway plus an authenticated
`/v1/models` — classifies the machine as **lab** (gateway directly reachable,
including on VPN) or **away**. The decision is made once per run and drives
the Spark path, the Hermes dashboard tunnel and the `argo-shim-all.sh`
wrapper. Away, spark-shim also keeps a tunnel to the Hermes dashboard on the
Windows box vr09 (`127.0.0.1:9119`); at the lab vr09 is reached without one.

On a Mac, a failed connect is cross-checked before it counts as "away": if
macOS Local Network privacy is what blocked it, spark-shim re-runs itself
under Apple's python (see [Troubleshooting](#troubleshooting)).

`titan` and `login-gce` are ssh_config aliases, not hostnames. Usernames and
keys live in `~/.ssh/config`. The `titan` alias itself carries no ProxyJump —
the off-site tunnel passes `-J login-gce` explicitly, so that hop is supplied
by this script, not the config. The publish relay also uses SSH (the chain
above), because ALCF login nodes cannot route to the gateway port at all.

`login-gce` on the Mac and `logins.cels.anl.gov` in publish mode are the same
host — the alias is just your Mac's ssh_config name for it.

Moving between home and the lab is safe: before starting one path,
spark-shim stops a leftover instance of the other path holding the port (it
never kills a process it doesn't own), and `--status` warns when the running
path doesn't match the detected location.

## Usage

    spark-shim                  # decide lab/away, serve 127.0.0.1:4100, configure opencode & hermes
    spark-shim --publish        # ALCF login node: relay for compute nodes (unchanged)
    spark-shim --where          # print "lab" or "away" (exit 0 / 1) — what the wrapper uses
    spark-shim --direct         # force the lab path (loopback relay), even away
    spark-shim --tunnel         # force the away path (ssh tunnel), even at the lab
    spark-shim --no-hermes      # skip the Hermes provider and its tunnel
    spark-shim --status         # location, paths, tunnels, configured addresses
    spark-shim --test           # real round-trip on both wire protocols
    spark-shim --models         # list models the gateway is serving
    spark-shim --stop           # tear down every tunnel and relay we own

`--port` overrides the port (default 4100, or `$SPARK_SHIM_PORT`).

After rewriting configs, spark-shim reminds you to restart OpenCode and
Hermes — a running session keeps the old config. If OpenCode is running at
that moment it also warns, with the PIDs: a live OpenCode can write the config
it loaded back over `opencode.json` and undo the change.

`--where` prints exactly `lab` or `away` on stdout and nothing else; notices
(gateway override, Local Network warnings) go to stderr. Scripts can rely on
`where=$(spark-shim --where 2>/dev/null)`.

## The lab relay

At the lab (or on VPN) the stable address is served by a small detached
relay process: plain stdlib TCP, listening on `127.0.0.1:4100` only,
forwarding to the gateway endpoint (honouring `--gateway`). No socat, no
ssh, no keys. It is this script re-invoked in a hidden relay mode, and it is
found, health-checked and stopped by matching its own listen spec in the
process arguments — the same discipline the ssh tunnels use.

The relay runs under the same python that launched spark-shim. On a Mac that
python needs Local Network access to reach titan, which is why spark-shim
switches to `/usr/bin/python3` when the probe finds it blocked, before the
relay is started.

## Hermes

spark-shim owns two Hermes things and nothing else:

- a **managed provider block** — `# >>> spark-shim >>>` … `# <<< spark-shim <<<`
  — inserted as the first item of `custom_providers` in
  `~/.hermes/config.yaml`, with the stable address, the gateway key and the
  discovered models (context lengths copied from the OpenCode entries when
  known). Re-runs replace the block; the rest of the file (comments,
  ordering, Hermes's own model config) is untouched. A timestamped backup is
  written before any change — and none when nothing changed.
- the **vr09 dashboard tunnel** (lab and away): `127.0.0.1:9119 →
  127.0.0.1:9119 on vr09`. The Hermes backend on vr09 is a python process
  bound to **loopback only**, so *nothing outside the box* — not even a
  Mac at the lab — can reach it directly; the only access is a tunnel to
  vr09's sshd (key-only, user `mvictoras`) dialing the box's own
  loopback. Away, the ssh rides the CELS jump (`-J login-gce`): the
  single Duo happens when the jump master is bootstrapped, after which
  every start and every laptop-sleep recovery multiplexes through the
  dedicated ControlPersist master at `~/.ssh/spark-shim-vr09` with no
  new auth. At the lab the same tunnel runs jump-free. If the away
  master is unexpectedly gone, spark-shim prints the exact bootstrap
  command instead of failing blindly — retry loops that made a missed
  Duo push escalate into an IP block are what killed the old design.
  Health is judged by traffic through the forward, so a
  listening-but-dead leftover stays detected and replaced; a foreign
  holder of the port is left alone. `--stop` closes the master too;
  a missing socket is a no-op and a stale one is unlinked.

Hermes's default model and its Argo provider entries are never touched.

## The wrapper

`argo-shim-all.sh` (this repo; `~/scripts/argo-shim-all.sh` is a symlink to
it) brings up Spark, Hermes and Argo with one command per location. It is
Mac-only, runs from any directory, and tolerates being run through the
symlink. Order: spark-shim first, then Argo — a failing Argo step can never
block Spark, and each step's failure is reported without skipping the rest.

- **lab**: `argo-shim --direct --restart --codex --port 17950` in the
  background (no tunnels, no Duo prompt, no OpenCode Argo tunnel).
- **away**: `argo-shim --opencode` (OpenCode's Argo tunnel, one Duo prompt),
  then `argo-shim --restart --codex --port 17950` in the background with a
  log in the state directory. A stale shim holding 17950 is replaced via
  `--restart`.

The wrapper decides the location with `spark-shim --where` and matches its
stdout literally, which is why `--where` keeps notices on stderr. The Argo
shim's output, including its SSH lockout messages, goes to
`~/.cache/spark-shim/argo-shim.log`.

The Hermes tunnel line the old wrapper had is gone — spark-shim owns it.

## Endpoint override

The gateway endpoint defaults to `titan.alcf.anl.gov:4000`. Point the whole
tool at a different one with:

    spark-shim --gateway llm.example.org:8000
    spark-shim --gateway http://llm.example.org:8000   # scheme optional
    SPARK_GATEWAY=llm.example.org:8000 spark-shim      # same thing via env

Every mode follows the override: the lab/away probe, the lab relay's far
end, the tunnel's far end, and the `--publish` relay destination. The SSH
path itself (the `titan`/`login-gce` aliases) is unchanged — edit
`~/.ssh/config` for that. The CLI flag wins over the environment variable;
port defaults to 4000 when omitted.

## ALCF compute-node recipe

ALCF compute nodes cannot reach titan at all — and neither can ALCF login
nodes: the gateway port is filtered from the compute-center zone (and, as it
turns out, from CELS hosts too — only titan's own loopback is guaranteed to
answer). On a login node run:

    spark-shim --publish

This picks the relay that fits, in order:

- **Gateway routable from here** (e.g. a VPN'd Mac): plain
  `socat TCP-LISTEN:4100,fork,reuseaddr,bind=0.0.0.0` to the gateway.
- **Gateway not routable, primary route (titan loopback)**: jump through the
  CELS login gateway and let *titan* dial the far end itself —
  `ssh -J $CELS_USERNAME@logins.cels.anl.gov -L 0.0.0.0:4100:127.0.0.1:4000 $SPARK_TITAN_USER@titan.alcf.anl.gov`.
  Same topology as the off-site Mac tunnel, bound on `0.0.0.0`. Only
  titan:22 must be reachable from CELS — the gateway port never has to be,
  which is why this route survives firewall changes that break the homes
  chain (verified working from Polaris, Oct 2026). The titan-side username
  defaults to your local username; override with `$SPARK_TITAN_USER`.
- **Fallback (homes.cels chain)**: dial the gateway from CELS space —
  `ssh -J $CELS_USERNAME@logins.cels.anl.gov -L 0.0.0.0:4100:<gateway> $CELS_USERNAME@homes.cels.anl.gov`.
  Tried only if the titan-loopback relay fails to answer.

Both ssh routes require MFA on first connect: with a terminal the prompt
appears inline. A `ControlPersist` master keeps the authentication alive, so
the token is typed once and later runs (including non-interactive ones)
multiplex without re-auth. A consecutive-failure counter (3) stops retrying
so a looping script cannot get the login node's IP blocked by CSPO;
bootstrap interactively once, then delete
`~/.cache/spark-shim/ssh-fail-count` to reset it.

Set `CELS_USERNAME` if your CELS username differs from your local one (and
`SPARK_TITAN_USER` for the titan side, if that differs too).

Either way the script verifies the gateway answers through the relay and
rewrites the shared-home client configs to `http://<login-node>:4100/v1`.
Compute nodes need nothing installed — they just use the shared configs. If
the compute-center firewall filters the relay port too, fall back to a
per-node SSH tunnel (see Security notes). Use the **short hostname**
(`polaris-login-02`, not the FQDN) from compute nodes — split DNS on ALCF
resolves the FQDN to an unrouteable address from there.

**Compute nodes behind an HTTP proxy** (ALCF compute nodes get `http_proxy`
from `/etc/profile.d` or a sourced `proxy.sh`): the relay's host must be
exempted in `no_proxy`, and listed in *every* form a client may resolve it
to. curl matches `no_proxy` entries literally, so the short name suffices;
opencode's Bun runtime expands the short name through the search domains
first and matches the expanded FQDN, so that flavor is needed too. Miss
either and the proxy answers the request itself with a **502** — the relay
and gateway are never reached:

    export no_proxy="$no_proxy,aurora-uan-0009,aurora-uan-0009.hostmgmt.cm.aurora.alcf.anl.gov"

On aurora the expanded name is the hostmgmt FQDN; on polaris it is
`polaris-login-0X.hsn.cm.polaris.alcf.anl.gov`. Cover all login nodes your
`--publish` might land on — `no_proxy` does not support wildcards.

The relay is detached from the launching shell (survives logout), is reused
if a healthy one is already on the port, and is stopped with
`spark-shim --stop`. If the port is held by something else, pick another
with `--port`. Publish mode is unchanged by the lab/away work: Hermes is
skipped entirely there.

## API key

Resolution order: `$SPARK_API_KEY`, then
`provider.spark.options.headers.Authorization` in
`~/.config/opencode/opencode.json`, then the built-in default.

In `opencode.json` the key is stored as a full header value,
`"Authorization": "Bearer sk-…"`, not as an `apiKey` field. To test it by hand
without printing it:

    KEY=$(python3 -c "import json,os; print(json.load(open(os.path.expanduser('~/.config/opencode/opencode.json')))['provider']['spark']['options']['headers']['Authorization'])")
    curl -s -H "Authorization: $KEY" http://127.0.0.1:4100/v1/models

## Client configuration

- **opencode**: `provider.spark.options.baseURL` -> `http://127.0.0.1:4100/v1`
  (or `<login-node>:4100` in publish mode). Models are discovered from the
  gateway's `/v1/models` and merged into the provider: existing entries
  (names, limits, reasoning variants) are preserved, new gateway models get
  minimal entries, and `glm-5.3-flash-exl3` stays the default with its
  reasoning variants. `--test` uses the default model.
- **hermes**: the managed `custom_providers` block described above.
- **codex / claude**: no longer configured by spark-shim — Argo owns both.
  On each configure run the old managed block in `~/.codex/config.toml` is
  stripped (with a backup); `~/.cache/spark-shim/claude-settings.backup.json`
  is left untouched.

## Environment variables

- `SPARK_GATEWAY` — gateway endpoint override (see above).
- `SPARK_SHIM_PORT` — default relay/tunnel port (see above).
- `SPARK_API_KEY` — gateway key (see API key section).
- `CELS_USERNAME` — CELS ssh username (publish mode and the wrapper; the
  wrapper exports it).
- `SPARK_TITAN_USER` — titan ssh username for the publish relay (defaults to
  local user).
- `SPARK_SHIM_REEXECED` — internal. Set when spark-shim re-runs itself under
  `/usr/bin/python3`, so it does so at most once. Don't set it yourself.

## Security notes

- The lab relay and the away tunnel are loopback-only; the `--publish`
  relay, however, is an unauthenticated TCP endpoint on 0.0.0.0 of a shared
  login node: anyone on the internal network can connect. The gateway's
  Bearer key is the only gate.
- The gateway speaks plain HTTP; ALCF-internal traffic is not encrypted.
  To encrypt the compute-node-to-login-node leg, tunnel over SSH from the
  compute node and point configs at localhost instead:

      ssh -N -f -L 127.0.0.1:4100:127.0.0.1:4100 <login-node>

## Tests

    python3 -m unittest discover -s tests -t .

Stdlib `unittest` only, no new dependencies. Tests never touch your real
configs, the real gateway, or real ssh: they run the CLI against a temporary
`HOME` with fakes at the process-spawning boundary, the wrapper against
stub executables, and the relay against a local echo server.

## Requirements

- python 3 (stdlib only)
- `socat` for `--publish` when the gateway is directly routable
- `ssh` and `lsof` for the off-site tunnel mode and the publish ssh relays

- macOS: the Xcode Command Line Tools (`xcode-select --install`), so
  `/usr/bin/python3` is a real interpreter for the Local Network fallback

## Troubleshooting

### Quick health check

    spark-shim --status        # location, what's listening, configured addresses
    spark-shim --test          # real round-trip on both wire protocols
    tail ~/.cache/spark-shim/argo-shim.log   # the Argo side

In `--status`, the **Client configuration** lines should show
`http://127.0.0.1:4100/v1` for both opencode and hermes (or
`<login-node>:4100` after `--publish`). Anything else means a client is
bypassing the stable address.

### Spark works from the shell but a client fails (macOS Local Network privacy)

**Symptom.** At the lab, `curl` to the gateway works, but a Python script or
OpenCode gets `No route to host` (errno 65, `EHOSTUNREACH`). On older
versions of spark-shim, `--where` or `--status` said **away** at the lab, and
the answer changed with the directory you ran it from.

**Cause.** At the lab your Mac and titan are on the same subnet
(140.221.17.0/24). macOS blocks programs that haven't been granted "Local
Network" access from reaching hosts on the local subnet. Apple's own binaries
(`curl`, `/usr/bin/python3`) are allowed; a pyenv or Homebrew python usually
isn't, and neither is a Homebrew `opencode`. Under pyenv the directory
matters: `~/.python-version` selects pyenv's python for everything under your
home directory, while elsewhere (`/tmp`) `python3` may resolve to Apple's.

**What spark-shim does.** When its connect fails with `EHOSTUNREACH` on
macOS, it checks with Apple's `curl`. If curl gets through, it prints a
warning on stderr and re-runs itself once under `/usr/bin/python3`, so the
location check and the lab relay work whichever `python3` the shebang finds.
If `/usr/bin/python3` isn't usable (no Command Line Tools), it explains the
fix and treats the gateway as unreachable.

**What you do.** Keep every client pointed at `127.0.0.1:4100`. Loopback
isn't affected by the block, and the relay behind it runs under an allowed
python. Pointing OpenCode straight at `titan.alcf.anl.gov:4000` is what
fails. To grant access instead, use System Settings > Privacy & Security >
Local Network; command-line programs that aren't app bundles don't always
appear there.

To check an interpreter yourself:

    python3 -c "import socket; socket.create_connection(('titan.alcf.anl.gov', 4000), 3); print('ok')"

### OpenCode's config keeps reverting to titan

**Symptom.** spark-shim reports `provider.spark.baseURL → http://127.0.0.1:4100/v1`,
but `--status` later shows opencode at `http://titan.alcf.anl.gov:4000/v1`,
and `opencode.json`'s modification time is after the spark-shim run.

**Cause.** A running OpenCode (the TUI, or the background
`opencode serve --service`) can write the config it loaded back to
`~/.config/opencode/opencode.json`. Combined with the Local Network block
above, that leaves OpenCode unable to reach Spark.

**Fix.** Quit every OpenCode process, re-run spark-shim (or the wrapper),
then start OpenCode again. spark-shim warns when it finds OpenCode running
during the rewrite.

    pgrep -lx opencode         # anything listed must be quit first

### The gateway reports `"db": "Not connected"`

`curl http://titan.alcf.anl.gov:4000/health/readiness` may show
`{"status":"healthy","db":"Not connected"}`. On its own this is not the
problem: keys still authenticate and `/v1/models` still answers. Test with a
real key (see [API key](#api-key)) before blaming the gateway. A request sent
with a missing or malformed key gets an auth error or `No connected db.`,
which looks like a gateway fault but isn't.

### SSH lockout messages in the Argo log

`~/.cache/spark-shim/argo-shim.log` accumulates across runs, so it may show
`could not establish the SSH tunnel` and a 15-minute cooldown from an earlier
**away** run. At the lab the wrapper runs Argo in direct mode with no SSH at
all, and those lines don't apply. Check the last
`argo-shim-all: at the lab` / `away` line printed by the wrapper to see which
mode is current. Away, don't keep restarting while the cooldown is active:
repeated failed logins can get the shared login node's IP blocked.

## Layout

`spark-shim.sh` is the canonical tool; `~/scripts/spark-shim` is a symlink
to it. `argo-shim-all.sh` is the wrapper; `~/scripts/argo-shim-all.sh` is a
symlink to that.
