# spark-shim

Reach the DGX LiteLLM gateway on `titan` from anywhere.

The gateway on `titan:4000` already speaks both wire protocols natively —
`/v1/chat/completions` (OpenAI) and `/v1/messages` (Anthropic, streaming SSE
included) — so unlike argo-shim there is no HTTP translation proxy here. The
only real problem is reachability, and this script bridges it: it puts a
plain local TCP port in front of the gateway and rewrites each client's
config to point at it.

## Topology

    home         ->  127.0.0.1:4100  ==ssh -J login-gce==>  titan:4000
    ALCF login   ->  titan.alcf.anl.gov:4000  (direct, no tunnel)
    ALCF compute ->  http://<login-node>:4100  (socat relay via --publish)

`titan` and `login-gce` are ssh_config aliases, not hostnames. Usernames and
keys live in `~/.ssh/config`. The `titan` alias itself carries no ProxyJump —
the off-site tunnel passes `-J login-gce` explicitly, so that hop is supplied
by this script, not the config. Only the off-site mode uses SSH at all: the
ALCF paths are direct TCP (relay) or nothing.

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

ALCF compute nodes cannot reach titan (or the internet) directly. On a
login node run:

    spark-shim --publish

That starts `socat TCP-LISTEN:4100,fork,reuseaddr,bind=0.0.0.0` forwarding
to `titan.alcf.anl.gov:4000`, verifies the gateway actually answers through
it, and rewrites the shared-home client configs to
`http://<login-node>:4100/v1`. Compute nodes need nothing installed — they
just use the shared configs.

The relay is detached from the launching shell (survives logout), is reused
if a healthy one is already on the port, and is stopped with
`spark-shim --stop`. If the port is held by something else, pick another
with `--port`.

Why socat and not an SSH tunnel: on the login node the gateway is reachable
directly, so a plain TCP relay is the minimal moving part — no keys, no
agent, and no CSPO lockout risk from failed SSH auths on a shared machine.
SSH's authentication protects tunnel creation, not tunnel use; a 0.0.0.0
listener has the same exposure either way.

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
- `socat` for `--publish` (ALCF login node)
- `ssh` and `lsof` for the off-site tunnel mode

## Layout

`spark-shim.sh` is the canonical tool; `~/scripts/spark-shim` is a symlink
to it.
