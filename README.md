# The Socket Exchange

COL334 Assignment 2 — a single-threaded, non-blocking TCP exchange server with
a trader client and a market-data client.

There is **no build step**. Everything is Python 3 using only the standard
library (`socket`, `select`). The files under `server/` and `client/` are
`/bin/sh` wrappers that `exec python3` on the matching file in `src/`.

## Requirements

- FreeBSD (the server and both clients use `kqueue` via `select.kqueue`, which
  is BSD-only — this will not run on Linux).
- Python 3.

```
pkg install -y python3
```

## Layout

```
server/run-server         launcher for the exchange server
client/run-trader         launcher for the trader client
client/run-market-data    launcher for the market-data client
src/server.py             exchange server: framing, order book, matching, kqueue loop
src/client_trader.py      trader client
src/client_market_data.py market-data client
experiment.py             experiment harness (experiments 1-8)
client_generation.py      bonus load generator: opens N idle connections
```

Make the launchers executable once after cloning:

```
chmod +x server/run-server client/run-trader client/run-market-data
```

## Running

```
./server/run-server [host] [port]        # default 127.0.0.1:5050
./client/run-trader      <host> <port> <name>
./client/run-market-data <host> <port> <instrument>
```

Both clients take either no arguments (defaults `127.0.0.1 5050`, trader name
`alice`, instrument `JNST`) or all three.

> **Port note.** `src/server.py` defaults to **5050**, deliberately avoiding
> 5000. The experiment harness overrides this and always starts the server on
> **5000**, so every observation command in the report filters on port 5000.

Example session:

```
# terminal 1
./server/run-server 127.0.0.1 5000

# terminal 2
./client/run-market-data 127.0.0.1 5000 JNST

# terminals 3 and 4
./client/run-trader 127.0.0.1 5000 alice
./client/run-trader 127.0.0.1 5000 bob
```

Then type `BUY JNST 10 238` in one trader and `SELL JNST 10 238` in the other.
The market-data client prints the resulting `TRADE` line.

## Protocol

Messages are newline-terminated ASCII. Instruments are `JNST` and `IMCT`.

| Command | Sent by | Reply |
| --- | --- | --- |
| `LOGIN <username>` | trader | `OK` |
| `BUY <instrument> <qty> <price>` | trader | `ORDER_ACCEPTED <id>` |
| `SELL <instrument> <qty> <price>` | trader | `ORDER_ACCEPTED <id>` |
| `CANCEL <order_id>` | trader | `ORDER_CANCELLED <id>` |
| `SUBSCRIBE <instrument>` | market data | `OK` |
| `UNSUBSCRIBE <instrument>` | market data | `OK` |
| `QUIT` | either | `OK`, then close |

Anything rejected produces `ERROR <reason>`.

A connection's role is decided by its first command: `LOGIN` makes it a
`TRADER`, `SUBSCRIBE` makes it `MARKET_DATA`, and after that the other role's
commands are refused. Usernames are unique across live connections.

Two orders match when they are on the same instrument, on opposite sides, and
at **identical** prices. On a match the server pushes:

- `TRADE <instrument> <qty> <price>` to every market-data client subscribed to
  that instrument,
- `BOUGHT <instrument> <qty> <price>` to the buying trader,
- `SOLD <instrument> <qty> <price>` to the selling trader.

## Design

- **Concurrency.** One process, one thread, one `kqueue` event loop. Every
  accepted socket is set non-blocking. No client can stall another because the
  server only ever reads sockets the kernel has reported as ready.
- **Framing.** TCP is a byte stream, so each `ClientSession` keeps an `in_buf`
  bytearray. `run()` appends whatever `recv()` returned and then repeatedly
  splits on `\n`, dispatching complete lines and keeping the partial tail for
  the next read.
- **Writes.** `ClientSession.send()` tries `conn.send()` directly. If the
  kernel send buffer is full it raises `BlockingIOError`; the unsent bytes go
  into that session's `out_buf` and a `KQ_FILTER_WRITE` filter is armed, so the
  loop finishes the write when the socket drains.
- **Disconnects.** FIN, RST and process death all converge on one path:
  `recv()` returns `b""` or raises, `run()` reports the session dead, and
  `cleanup_session()` drops it from the session table, releases its username
  and closes the descriptor.

### Known limitations

- `out_buf` is unbounded. A market-data client that stops reading forever will
  make the server's memory grow without limit; a production design would cap
  the backlog and disconnect the client.
- The server writes a verbose trace to stdout on every accept, match and
  buffered write. This is useful evidence during the experiments but is a real
  throughput cost under load.

## Experiments

The harness starts and stops the server itself. Run it **from the repo root**,
because it invokes `./server/run-server`:

```
python3 experiment.py <1-8>
```

Experiments 3 and 5 inspect the server's syscalls. `server/run-server` honours
an optional `EXCHANGE_KTRACE` environment variable: when it is set, the server
is run under `ktrace` writing to that path. When it is unset — which is the
normal case — the launcher behaves exactly as it otherwise would.

```
env EXCHANGE_KTRACE=$PWD/ktrace/exp3.ktr python3 experiment.py 3
kdump -f $PWD/ktrace/exp3.ktr | grep -B1 -A2 recvfrom
```

Kill any stale server before each run: `pkill -f src/server.py`.

## Bonus: idle connection scaling

`client_generation.py` opens a requested number of idle connections and holds
them so the server's per-connection cost can be measured. It raises its own
file-descriptor limit and reports failures grouped by `errno`, which is what
identifies the binding constraint.

```
python3 client_generation.py 127.0.0.1 5000 10000 127.0.0.1 127.0.0.2 127.0.0.3
```

Going beyond roughly 64,500 connections requires extra source addresses,
because connections to one (host, port) pair are distinguished only by their
source port:

```
ifconfig lo0 alias 127.0.0.2/32
ifconfig lo0 alias 127.0.0.3/32
```

Remove them again with `ifconfig lo0 -alias 127.0.0.2` when finished.
