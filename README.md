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
Hermes — a running session keeps the old config.

## The lab relay

At the lab (or on VPN) the stable address is served by a small detached
relay process: plain stdlib TCP, listening on `127.0.0.1:4100` only,
forwarding to the gateway endpoint (honouring `--gateway`). No socat, no
ssh, no keys. It is this script re-invoked in a hidden relay mode, and it is
found, health-checked and stopped by matching its own listen spec in the
process arguments — the same discipline the ssh tunnels use.

## Hermes

spark-shim owns two Hermes things and nothing else:

- a **managed provider block** — `# >>> spark-shim >>>` … `# <<< spark-shim <<<`
  — inserted as the first item of `custom_providers` in
  `~/.hermes/config.yaml`, with the stable address, the gateway key and the
  discovered models (context lengths copied from the OpenCode entries when
  known). Re-runs replace the block; the rest of the file (comments,
  ordering, Hermes's own model config) is untouched. A timestamped backup is
  written before any change — and none when nothing changed.
- the **vr09 dashboard tunnel** (away only): `127.0.0.1:9119 →
  vr09.alcf.anl.gov:9119` through `login-gce`, on a dedicated
  non-multiplexed connection. Health is judged by traffic through the
  forward, so a listening-but-dead leftover from a laptop sleep is detected
  and replaced. At the lab nothing is started and nothing is killed.

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

## Layout

`spark-shim.sh` is the canonical tool; `~/scripts/spark-shim` is a symlink
to it. `argo-shim-all.sh` is the wrapper; `~/scripts/argo-shim-all.sh` is a
symlink to that.
