# spark-shim

Reach the DGX LiteLLM gateway on `titan` from anywhere.

The gateway on `titan:4000` already speaks both wire protocols natively —
`/v1/chat/completions` (OpenAI) and `/v1/messages` (Anthropic, streaming SSE
included) — so unlike argo-shim there is no HTTP translation proxy here. The
only real problem is reachability, and this script bridges it: it puts a
plain local TCP port in front of the gateway and rewrites each client's
config to point at it.

## Topology

    home (VPN)   ->  titan.alcf.anl.gov:4000  (direct, no tunnel)
    home (off-site) ->  127.0.0.1:4100  ==ssh -J login-gce==>  titan:4000
    ALCF login   ->  0.0.0.0:4100 ==ssh -J logins.cels==> titan:4000  (MFA once)
    ALCF compute ->  http://<login-node>:4100  (relay via --publish)

`titan` and `login-gce` are ssh_config aliases, not hostnames. Usernames and
keys live in `~/.ssh/config`. The `titan` alias itself carries no ProxyJump —
the off-site tunnel passes `-J login-gce` explicitly, so that hop is supplied
by this script, not the config. The publish relay also uses SSH (the chain
above), because ALCF login nodes cannot route to the gateway port at all.

`login-gce` on the Mac and `logins.cels.anl.gov` in publish mode are the same
host — the alias is just your Mac's ssh_config name for it.

## Usage

    spark-shim                  # connect (auto: direct vs tunnel) + configure opencode & codex
    spark-shim --publish        # ALCF login node: socat relay for compute nodes
    spark-shim --claude         # ...and repoint Claude Code (displaces argo; backed up)
    spark-shim --status         # what's running, what's configured
    spark-shim --test           # real round-trip on both wire protocols
    spark-shim --models         # list models the gateway is serving
    spark-shim --stop           # tear the tunnel and/or relay down
    spark-shim --restore-claude # put argo's Claude Code settings back

`--port` overrides the port (default 4100, or `$SPARK_SHIM_PORT`).

## Endpoint override

The gateway endpoint defaults to `titan.alcf.anl.gov:4000`. Point the whole
tool at a different one with:

    spark-shim --gateway llm.example.org:8000
    spark-shim --gateway http://llm.example.org:8000   # scheme optional
    SPARK_GATEWAY=llm.example.org:8000 spark-shim      # same thing via env

Every mode follows the override: reachability detection, the direct base
URL, the tunnel's far end, and the `--publish` relay destination. The SSH
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

The relay is detached from the launching shell (survives logout), is reused
if a healthy one is already on the port, and is stopped with
`spark-shim --stop`. If the port is held by something else, pick another
with `--port`.

## API key

Resolution order: `$SPARK_API_KEY`, then
`provider.spark.options.headers.Authorization` in
`~/.config/opencode/opencode.json`, then the built-in default.

## Client configuration

- **opencode**: `provider.spark.options.baseURL` -> `<base>/v1`.
- **codex**: a managed `# >>> spark-shim >>>` block in
  `~/.codex/config.toml` (`model_providers.spark` plus `spark` and
  `spark-fast` profiles).
- **claude** (only with `--claude`): `ANTHROPIC_BASE_URL` in
  `~/.claude/settings.json`, with the displaced (argo) settings backed up to
  `~/.cache/spark-shim/claude-settings.backup.json` and restorable via
  `--restore-claude`. Claude Code has exactly one base URL, so argo and
  spark cannot both own it.

## Environment variables

- `SPARK_GATEWAY` — gateway endpoint override (see above).
- `SPARK_SHIM_PORT` — default relay/tunnel port (see above).
- `SPARK_API_KEY` — gateway key (see API key section).
- `CELS_USERNAME` — CELS ssh username (publish mode; defaults to local user).
- `SPARK_TITAN_USER` — titan ssh username for the publish relay (defaults to
  local user).

## Security notes

- The `--publish` relay is an unauthenticated TCP endpoint on 0.0.0.0 of a
  shared login node: anyone on the internal network can connect. The
  gateway's Bearer key is the only gate.
- The gateway speaks plain HTTP; ALCF-internal traffic is not encrypted.
  To encrypt the compute-node-to-login-node leg, tunnel over SSH from the
  compute node and point configs at localhost instead:

      ssh -N -f -L 127.0.0.1:4100:127.0.0.1:4100 <login-node>

## Requirements

- python 3 (stdlib only)
- `socat` for `--publish` when the gateway is directly routable
- `ssh` and `lsof` for the off-site tunnel mode and the publish ssh relays

## Layout

`spark-shim.sh` is the canonical tool; `~/scripts/spark-shim` is a symlink
to it.
