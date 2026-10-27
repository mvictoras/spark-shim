"""The relay data path: real bytes through the real hidden relay mode.

The one place we exercise the actual process rather than fakes: the relay is
spawned exactly as spark-shim starts it in the field, against a local echo
server, and must pass bytes both ways and exit cleanly on SIGTERM.
"""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
import time
import shutil
from pathlib import Path

from tests.helpers import SCRIPT


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class EchoServer(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        self.running = True

    def run(self):
        self.sock.settimeout(0.5)
        while self.running:
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self.echo, args=(conn,), daemon=True).start()

    @staticmethod
    def echo(conn):
        try:
            while True:
                data = conn.recv(65536)
                if not data:
                    break
                conn.sendall(data)
        except OSError:
            pass
        finally:
            conn.close()

    def stop(self):
        self.running = False
        self.sock.close()


class RelayDataPathTest(unittest.TestCase):
    def setUp(self):
        self.echo = EchoServer()
        self.echo.start()
        self.relay_port = free_port()
        self.proc = subprocess.Popen(
            [
                sys.executable,
                SCRIPT,
                "--_relay",
                "--port",
                str(self.relay_port),
                "--gateway",
                f"127.0.0.1:{self.echo.port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def tearDown(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.echo.stop()

    def wait_listening(self, port, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return True
            except OSError:
                time.sleep(0.1)
        self.fail(f"relay never listened on {port}")

    def roundtrip(self, payload):
        with socket.create_connection(("127.0.0.1", self.relay_port), timeout=10) as s:
            s.sendall(payload)
            s.shutdown(socket.SHUT_WR)
            received = b""
            while len(received) < len(payload):
                chunk = s.recv(65536)
                if not chunk:
                    break
                received += chunk
        return received

    def test_bytes_pass_both_ways_and_sigterm_exits_cleanly(self):
        self.wait_listening(self.relay_port)
        # Client -> relay -> echo -> relay -> client (both pipe directions).
        self.assertEqual(self.roundtrip(b"hello through the relay"), b"hello through the relay")
        self.assertEqual(self.roundtrip(b"second connection, same relay"), b"second connection, same relay")

        # Clean shutdown on SIGTERM, like --stop and the switch-over send it.
        self.proc.send_signal(signal.SIGTERM)
        self.assertEqual(self.proc.wait(timeout=10), 0)
        # And the port is actually released.
        time.sleep(0.3)
        with self.assertRaises(OSError):
            with socket.create_connection(("127.0.0.1", self.relay_port), timeout=1):
                pass


if __name__ == "__main__":
    unittest.main()
