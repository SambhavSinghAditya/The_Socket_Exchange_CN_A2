import socket, select
import sys

HOST = "127.0.0.1"
PORT = 5050


def parse_args(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if len(args) == 0:
        return HOST, PORT, "JNST"
    if len(args) != 3:
        raise SystemExit("Usage: client_market_data.py [host port instrument]")
    host, port, instrument = args
    return host, int(port), instrument


# def read_lines(conn):
#     """
#     Same framer as the server -- reused here because the client faces
#     the exact same byte-stream problem when reading the server's replies.
#     """
#     buffer = b""
#     while True:
#         while b"\n" in buffer:
#             line, buffer = buffer.split(b"\n", 1)
#             yield line.decode(errors="replace")

#         data = conn.recv(4096)
#         if not data:
#             return  # server closed the connection
#         buffer += data

def read(sock, n):
    data=b""
    while True: 
        try:
            chunk=sock.recv(n)
        except BlockingIOError:
            break
        except (ConnectionResetError, OSError):
            exit(1)
        if chunk==b"":
            # gracefully closing the socket
            print("Server closed the connection, gracefully")
            exit(1)
        data+=chunk
    return data


#edited
def print_lines(in_buf):
    """Print every complete line in in_buf, return the leftover partial line."""
    while b"\n" in in_buf:
        line, in_buf = in_buf.split(b"\n", 1)
        print(line.decode(errors="replace"))
    return in_buf


#edited
def graceful_quit(sock, in_buf, write_buffer):
    """Flush QUIT, wait for the server's reply, then half-close (FIN, not RST)."""
    sock.setblocking(True)
    sock.settimeout(2.0)
    try:
        if write_buffer:
            sock.sendall(write_buffer)
        while b"\n" not in in_buf:
            chunk = sock.recv(4096)
            if chunk == b"":
                break
            in_buf += chunk
    except OSError:
        pass
    print_lines(in_buf)
    try:
        sock.shutdown(socket.SHUT_WR)
    except OSError:
        pass
    sock.close()


def main():
    host, port, instrument = parse_args()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(f"SUBSCRIBE {instrument}\n".encode())
    print(f"Connected to {host}:{port}")
    print(f"Subscribed to {instrument}.")
    #edited
    print("Type SUBSCRIBE <instrument> / UNSUBSCRIBE <instrument> to change subscriptions.")
    #edited
    print("Type QUIT to disconnect, or Ctrl+C to force-exit.")
    print("Waiting for trade updates...\n")

    kq=select.kqueue()
    sock.setblocking(False)

    event=select.kevent(
        sock.fileno(),
        filter= select.KQ_FILTER_READ, 
        flags= select.KQ_EV_ADD,
    )
    event2= select.kevent(
        sys.stdin.fileno(),
        filter= select.KQ_FILTER_READ, 
        flags= select.KQ_EV_ADD,
    )
    kq.control([event, event2], 0, 0) 
    # this 0,0 means "no max event" as this control is not for returning any event, and no timeout, as we want to block until an event occurs
    write_buffer= b""
    #edited
    in_buf= b""
    while True: 
        events= kq.control(None, 8, None)
        for event in events:
            if event.filter==select.KQ_FILTER_WRITE and event.ident==sock.fileno():
                # socket is ready to write:
                if write_buffer:
                    bytes_sent=sock.send(write_buffer)
                    write_buffer=write_buffer[bytes_sent:]
                if not write_buffer:
                    # no more data to write, remove the write event
                    kq.control([select.kevent(
                        sock.fileno(),
                        filter=select.KQ_FILTER_WRITE,
                        flags=select.KQ_EV_DELETE
                    )], 0, 0)
            elif event.filter==select.KQ_FILTER_READ and event.ident==sock.fileno():
                # socket got something to read
                #edited
                in_buf += read(sock, 4096)
                #edited
                in_buf = print_lines(in_buf)
            elif event.filter==select.KQ_FILTER_READ and event.ident==sys.stdin.fileno():
                # user typed something -- SUBSCRIBE / UNSUBSCRIBE / QUIT are all
                # permitted for a market-data client (protocol spec 2.4)
                #edited
                raw=sys.stdin.readline()
                #edited
                if raw=="":
                    #edited
                    # EOF on stdin (Ctrl-D): treat as QUIT instead of spinning
                    line="QUIT"
                #edited
                else:
                    #edited
                    line=raw.strip()
                    #edited
                    if not line:
                        #edited
                        continue
                payload=(line+"\n").encode()
                #edited
                try:
                    bytes_sent=sock.send(payload)
                #edited
                except BlockingIOError:
                    #edited
                    bytes_sent=0
                #edited
                except (BrokenPipeError, OSError) as e:
                    #edited
                    print(f"Send failed, server is gone: {e}")
                    #edited
                    sock.close()
                    #edited
                    exit(1)
                if bytes_sent<len(payload):
                    write_buffer=payload[bytes_sent:]
                    write_ev=select.kevent(
                        sock.fileno(), 
                        filter=select.KQ_FILTER_WRITE,
                        flags=select.KQ_EV_ADD
                    )
                    kq.control([write_ev], 0, 0)

                if line.upper()=="QUIT":
                    print("Disconnecting...")
                    #edited
                    graceful_quit(sock, in_buf, write_buffer)
                    exit(0)

if __name__ == "__main__":
    main()