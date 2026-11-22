"""Median time (ms) from launching a server to it accepting TCP connections, over 3 starts.

Usage: python3 startup.py PORT WORKDIR CMD...
"""

import socket
import statistics
import subprocess
import sys
import time

port, cwd, cmd = int(sys.argv[1]), sys.argv[2], sys.argv[3:]
runs = []
for _ in range(3):
    t0 = time.perf_counter()
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = t0 + 120
    while time.perf_counter() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.05).close()
            break
        except OSError:
            time.sleep(0.002)
    runs.append((time.perf_counter() - t0) * 1000)
    p.terminate()
    try:
        p.wait(15)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    time.sleep(1)  # let ports close
print(round(statistics.median(runs)))
