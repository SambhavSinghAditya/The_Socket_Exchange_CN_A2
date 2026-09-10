#!/usr/bin/env python3
"""
client_generation.py

Load generator for the bonus question: open as many idle TCP connections to
the Exchange Server as the system will allow, then hold them open so the
server's per-connection cost can be measured.

Usage:
    python3 client_generation.py <host> <port> <count> [src_addr ...]

The connections are deliberately idle -- they never send LOGIN or SUBSCRIBE.
On the server each one is an accepted socket with a ClientSession whose
client_type stays None, which is exactly the idle-connection cost we want.

A single source address can only produce about 64,500 connections to one
(host, port) pair, because the four-tuple only varies in the source port.
Pass several source addresses to go past that; add them first with:

    ifconfig lo0 alias 127.0.0.2/32
    ifconfig lo0 alias 127.0.0.3/32
"""

import errno
import resource
import signal
import socket
import sys
import time


REPORT_EVERY = 5000


def raise_fd_limit():
    """Lift RLIMIT_NOFILE to the hard limit so we are not the bottleneck."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= hard:
        return soft
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        return hard
    except (ValueError, OSError):
        return soft


def parse_args():
    if len(sys.argv) < 4:
        raise SystemExit(
            "Usage: client_generation.py <host> <port> <count> [src_addr ...]"
        )
    host = sys.argv[1]
    port = int(sys.argv[2])
    count = int(sys.argv[3])
    sources = sys.argv[4:] or [None]
    return host, port, count, sources


def describe(err_number):
    """Turn an errno into the name the report should quote."""
    if err_number is None:
        return "unknown"
    return errno.errorcode.get(err_number, str(err_number))


def main():
    host, port, count, sources = parse_args()

    limit = raise_fd_limit()
    print(f"File-descriptor limit for this process: {limit}")
    print(f"Opening up to {count} connections to {host}:{port} "
          f"from {len(sources)} source address(es)...")

    held = []
    failures = {}

    for i in range(count):
        # socket() itself can fail once a kernel-wide limit is reached (ENOBUFS
        # when the socket zone or mbuf clusters run out), so it has to be inside
        # the try -- otherwise the run dies instead of reporting the limit.
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            source = sources[i % len(sources)]
            if source is not None:
                sock.bind((source, 0))
            sock.connect((host, port))
            # Keep a reference: if the socket were garbage collected the
            # kernel would close it and the connection would disappear.
            held.append(sock)
        except OSError as exc:
            name = describe(exc.errno)
            failures[name] = failures.get(name, 0) + 1
            if sock is not None:
                sock.close()

        if (i + 1) % REPORT_EVERY == 0:
            print(f"  attempted {i + 1}, holding {len(held)}")

    print()
    print(f"Opened {len(held)} connections out of {count} attempted.")

    if failures:
        print("Failures by errno:")
        for name, n in sorted(failures.items(), key=lambda kv: -kv[1]):
            print(f"  {name}: {n}")
        print()
        print("  EMFILE / ENFILE -> file-descriptor limit reached")
        print("  ENOBUFS         -> kernel socket zone / mbuf clusters exhausted")
        print("  EADDRNOTAVAIL   -> source address not on an interface, or")
        print("                     ephemeral source ports exhausted")
        print("  ECONNREFUSED    -> server accept queue overflowed")
        print("  ETIMEDOUT       -> server could not keep up with accept()")
    else:
        print("No failures.")

    print()
    print(f"Holding {len(held)} idle connections")
    print("Take your measurements now. Press Ctrl-C to release them.")

    signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nReleasing connections.")


if __name__ == "__main__":
    main()
