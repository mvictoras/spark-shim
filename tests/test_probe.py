"""The real reachability probe.

`detect_direct` is the one lab/away decision, so every other test fakes it.
This module is the narrow exception (like the relay data-path test): it runs
the actual probe against a local fake gateway and pins the property that
makes the decision safe — lab requires a successful *authenticated* model
list, never merely an open TCP port — and that `--where` reads the same
single probe the connect path uses.
"""

import contextlib
import errno
import io
import json
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler

try:
    from http.server import ThreadingHTTPServer  # Python 3.7+
except ImportError:  # Python 3.6
    from http.server import HTTPServer
    from socketserver import ThreadingMixIn

    class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True

from tests.helpers import load_module


class FakeGatewayHandler(BaseHTTPRequestHandler):
    """Stands in for titan:4000. Behaviour is set per test via class attrs."""

    valid_key = "sk-test"
    behaviour = "gateway"  # "gateway" | "html" | "silent"
    requests = None  # replaced per test with a shared recording list

    def do_GET(self):
        type(self).requests.append(self.headers.get("Authorization"))
        if type(self).behaviour == "html":
            # An open port running something that is not the gateway.
            body = b"<html>definitely not litellm</html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
        elif self.headers.get("Authorization") != f"Bearer {type(self).valid_key}":
            self.send_response(401)
            body = b""
        else:
            body = json.dumps({"data": [{"id": "glm-5.3-flash-exl3"}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle(self):
        if type(self).behaviour == "silent":
            # Accept the TCP connect, then close without a byte: the
            # something-else-is-listening case the probe must see through.
            self.connection.close()
            return
        super().handle()

    def log_message(self, *args):
        pass


class ProbeTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.mkdtemp(prefix="spark-shim-probe-")
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        self.mod = load_module(tmp)

    def start_gateway(self, behaviour="gateway", valid_key="sk-test"):
        """Serve a fake gateway on an ephemeral port; (server, handler)."""
        attrs = {"behaviour": behaviour, "valid_key": valid_key, "requests": []}
        handler = type("Handler", (FakeGatewayHandler,), attrs)
        srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv, handler

    def stop_gateway(self, srv):
        # Idempotent with the cleanups, so tests may stop it early too.
        srv.shutdown()
        srv.server_close()

    def point_probe_at(self, host, port):
        self.mod.GATEWAY_HOST = host
        self.mod.GATEWAY_PORT = port

    def set_key_env(self, key="sk-test"):
        old = os.environ.pop("SPARK_API_KEY", None)
        os.environ["SPARK_API_KEY"] = key
        if old is None:
            self.addCleanup(os.environ.pop, "SPARK_API_KEY", None)
        else:
            self.addCleanup(os.environ.__setitem__, "SPARK_API_KEY", old)

    def run_where(self, *flags):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            with self.assertRaises(SystemExit) as ctx:
                self.mod.main(["--where", *flags])
        return out.getvalue().strip(), ctx.exception.code

    def test_authenticated_model_list_is_lab(self):
        srv, _ = self.start_gateway()
        self.point_probe_at("127.0.0.1", srv.server_address[1])
        self.assertTrue(self.mod.detect_direct("sk-test"))

    def test_open_port_that_rejects_the_key_is_away(self):
        # A port being open proves nothing: the gateway answers 401 without
        # the right key, and the probe must read that as "away".
        srv, _ = self.start_gateway(valid_key="sk-other")
        self.point_probe_at("127.0.0.1", srv.server_address[1])
        self.assertFalse(self.mod.detect_direct("sk-test"))

    def test_open_port_serving_html_is_away(self):
        # Positive identification means a parseable model list, not any
        # 200 response — another service on the port cannot impersonate it.
        srv, _ = self.start_gateway(behaviour="html")
        self.point_probe_at("127.0.0.1", srv.server_address[1])
        self.assertFalse(self.mod.detect_direct("sk-test"))

    def test_silent_port_is_away(self):
        # Accepts the connect, then closes without a byte.
        srv, _ = self.start_gateway(behaviour="silent")
        self.point_probe_at("127.0.0.1", srv.server_address[1])
        self.assertFalse(self.mod.detect_direct("sk-test"))

    def test_closed_port_is_away_and_sends_no_request(self):
        # A refused connect is "away" before any HTTP is attempted.
        srv, handler = self.start_gateway()
        self.stop_gateway(srv)
        self.point_probe_at("127.0.0.1", srv.server_address[1])
        self.assertFalse(self.mod.detect_direct("sk-test"))
        self.assertEqual(handler.requests, [])

    def test_probe_sends_bearer_credentials(self):
        srv, handler = self.start_gateway()
        self.point_probe_at("127.0.0.1", srv.server_address[1])
        self.mod.gateway_responds("127.0.0.1", srv.server_address[1], "sk-test")
        self.assertEqual(handler.requests, ["Bearer sk-test"])

    def test_where_reads_the_same_probe_as_the_connect_path(self):
        # End to end through main(): a gateway that authenticates and serves
        # a model list is lab (exit 0); the same port rejecting the key is
        # away (exit 1). No probe fakes are involved.
        self.set_key_env("sk-test")
        srv, _ = self.start_gateway()
        self.point_probe_at("127.0.0.1", srv.server_address[1])
        self.assertEqual(self.run_where(), ("lab", 0))
        self.stop_gateway(srv)

        srv2, _ = self.start_gateway(valid_key="sk-other")
        self.point_probe_at("127.0.0.1", srv2.server_address[1])
        self.assertEqual(self.run_where(), ("away", 1))


class Reexeced(Exception):
    """Raised by the fake exec seam: a real execve never returns."""


class LocalNetworkBlockTestCase(unittest.TestCase):
    """macOS Local Network privacy: EHOSTUNREACH for us, but Apple's curl connects.

    Seams faked: the TCP connect (connect_error), the curl cross-check, the
    /usr/bin/python3 availability check and the exec itself. The platform is
    pinned to darwin so the tests run the same on Linux CI.
    """

    def setUp(self):
        tmp = tempfile.mkdtemp(prefix="spark-shim-lnp-")
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        self.mod = load_module(tmp)
        self.mod.connect_error = lambda *a, **k: errno.EHOSTUNREACH
        self.mod.system_can_connect = lambda host, port: True
        self.mod.system_python_usable = lambda: True
        self.execs = []

        def fake_exec(path, argv, env):
            self.execs.append((path, argv, env))
            raise Reexeced()

        self.mod.exec_replace = fake_exec
        patcher = mock.patch.object(self.mod.sys, "platform", "darwin")
        patcher.start()
        self.addCleanup(patcher.stop)
        env = mock.patch.dict(os.environ, {"SPARK_API_KEY": "sk-test"})
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop(self.mod.REEXEC_ENV, None)

    def run_where(self):
        out, err = io.StringIO(), io.StringIO()
        code = None
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                self.mod.main(["--where", "--gateway", "titan.example:4000"])
            except SystemExit as exc:
                code = exc.code
        return out.getvalue(), err.getvalue(), code

    def test_block_reexecs_under_system_python_with_same_argv(self):
        with self.assertRaises(Reexeced):
            self.run_where()
        self.assertEqual(len(self.execs), 1)
        path, argv, env = self.execs[0]
        self.assertEqual(path, self.mod.SYSTEM_PYTHON)
        self.assertEqual(argv[0], self.mod.SYSTEM_PYTHON)
        self.assertTrue(argv[1].endswith("spark-shim.sh"))
        self.assertEqual(argv[2:], ["--where", "--gateway", "titan.example:4000"])
        self.assertEqual(env[self.mod.REEXEC_ENV], "1")

    def test_block_after_reexec_warns_on_stderr_and_keeps_where_parseable(self):
        # Already re-exec'd once (or no usable system python): no loop, a
        # clear explanation on stderr, and stdout is still exactly "away" --
        # the wrapper parses it.
        os.environ[self.mod.REEXEC_ENV] = "1"
        out, err, code = self.run_where()
        self.assertEqual(self.execs, [])
        self.assertEqual(out.strip(), "away")
        self.assertEqual(code, 1)
        self.assertIn("Local Network", err)

    def test_no_system_python_warns_instead_of_reexec(self):
        self.mod.system_python_usable = lambda: False
        out, err, code = self.run_where()
        self.assertEqual(self.execs, [])
        self.assertEqual(out.strip(), "away")
        self.assertIn("Privacy & Security", err)

    def test_genuinely_unreachable_is_plain_away(self):
        # curl can't connect either: really off-site, no warning, no exec.
        self.mod.system_can_connect = lambda host, port: False
        out, err, code = self.run_where()
        self.assertEqual(self.execs, [])
        self.assertEqual((out.strip(), code), ("away", 1))
        self.assertNotIn("Local Network", err)

    def test_only_ehostunreach_on_macos_counts(self):
        calls = []
        self.mod.system_can_connect = lambda h, p: calls.append((h, p)) or True
        self.assertFalse(
            self.mod.local_network_blocked("h", 1, errno.ECONNREFUSED)
        )
        with mock.patch.object(self.mod.sys, "platform", "linux"):
            self.assertFalse(
                self.mod.local_network_blocked("h", 1, errno.EHOSTUNREACH)
            )
        self.assertEqual(calls, [])  # curl is never spawned for these
        self.assertTrue(self.mod.local_network_blocked("h", 1, errno.EHOSTUNREACH))


if __name__ == "__main__":
    unittest.main()
