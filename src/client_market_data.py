
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

def main():
    host, port, instrument = parse_args()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(f"SUBSCRIBE {instrument}\n".encode())
    print(f"Connected to {host}:{port}")
    print(f"Subscribed to {instrument}.")
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
                data=read(sock, 4096).decode(errors="replace").strip()
                print(data)
            elif event.filter==select.KQ_FILTER_READ and event.ident==sys.stdin.fileno():
                # user typed something
                line=sys.stdin.readline().strip()
                if not line:
                    continue
                payload=(line+"\n").encode()
                bytes_sent=sock.send(payload)
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
                    sock.close()
                    exit(0)

if __name__ == "__main__":
    main()