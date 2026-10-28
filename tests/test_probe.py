"""The real reachability probe.

`detect_direct` is the one lab/away decision, so every other test fakes it.
This module is the narrow exception (like the relay data-path test): it runs
the actual probe against a local fake gateway and pins the property that
makes the decision safe — lab requires a successful *authenticated* model
list, never merely an open TCP port — and that `--where` reads the same
single probe the connect path uses.
"""

import contextlib
import io
import json
import os
import shutil
import tempfile
import threading
import unittest
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


if __name__ == "__main__":
    unittest.main()
