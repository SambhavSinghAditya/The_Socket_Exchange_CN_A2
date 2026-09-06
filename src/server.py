"""
Exchange Server -- Checkpoint 2b
Covers: Lecture 1.3 (socket creation), 2.1-2.2 (framing), 2.3 (parsing + order book)

NOTE: This version accepts and fully services ONE client at a time.
Multi-client concurrency (threads / select) is Day 3 -- not covered yet.
"""

import socket, select
from dataclasses import dataclass, field

HOST = "127.0.0.1"
PORT = 5050  # avoiding 5000 -- often squatted by other services (see Lecture 1.3 notes)

VALID_INSTRUMENTS = {"JNST", "IMCT"}

executed_orders = [] # List of executed orders for logging / auditing


# ---------------------------------------------------------------------------
# Data model (Lecture 2.3)
# ---------------------------------------------------------------------------

@dataclass
class Order:
    order_id: int
    trader_name: str
    side: str          # "BUY" or "SELL"
    instrument: str
    qty: int
    price: int

@dataclass 
class Trade:
    seller_name: str
    buyer_name: str
    instrument: str
    qty: int
    price: int


class OrderBook:
    """
    Holds all outstanding orders, organized per-instrument and per-side
    so that matching (added in a later lecture) only ever has to scan
    the opposite side of the same instrument.
    """

    def __init__(self):
        self.next_order_id = 0
        self.orders_by_id = {}                                  # order_id -> Order
        self.buy_orders = {inst: [] for inst in VALID_INSTRUMENTS}   # instrument -> list[Order]
        self.sell_orders = {inst: [] for inst in VALID_INSTRUMENTS}  # instrument -> list[Order]
        # stores buy and sell orders list separately

    def new_order_id(self):
        oid = self.next_order_id
        self.next_order_id += 1
        return oid

    def remove_order(self, order: Order):
        book = self.buy_orders if order.side == "BUY" else self.sell_orders
        if order in book[order.instrument]:
            book[order.instrument].remove(order)
            print(f"Order {order.order_id} removed from book.")
            return True
        else:
            print(f"Warning: Order {order.order_id} not found in book for removal.")
            return False

    # def insert_order(self, order: Order):
    #     self.orders_by_id[order.order_id] = order
    #     book = self.buy_orders if order.side == "BUY" else self.sell_orders
    #     other_book = self.buy_orders if order.side == "SELL" else self.sell_orders
    #     executed = [] # list of executed orders
    #     tmp=order.qty
    #     for e in other_book[order.instrument]:
    #         if e.price==order.price: 
    #             print(f"Matching order found: {e} and {order}")
    #             if e.qty >= order.qty:
    #                 e.qty -= order.qty
    #                 executed_order= copy.copy(e)
    #                 executed_order.qty=order.qty
    #                 order.qty = 0

    #                 executed.append(executed_order)

    #                 print(f"Order {order.order_id} fully matched and removed from book.")
    #                 break
    #             else: 
    #                 order.qty -= e.qty

    #                 executed_order= copy.copy(e)

    #                 e.qty = 0

    #                 executed.append(executed_order)

    #                 print(f"Order {e.order_id} fully matched and removed from book.")
    #                 other_book[order.instrument].remove(e)
    #                 self.orders_by_id.pop(e.order_id, None)
    #                 continue
    #     if order.qty > 0:
    #         self.orders_by_id[order.order_id] = order
    #         book[order.instrument].append(order)
    #         order_executed = copy.copy(order)
    #         order_executed.qty = tmp - order.qty
    #         executed.append(order_executed)
    #         return executed
    #     else: 
    #         order_executed = copy.copy(order)
    #         order_executed.qty = tmp
    #         executed.append(order_executed)
    #         return executed

    def insert_order(self, order: Order):
        book = self.buy_orders if order.side == "BUY" else self.sell_orders
        other_book = self.buy_orders if order.side == "SELL" else self.sell_orders
        executed = [] # list of executed orders
        # filled = 0
        tmp = order.qty


        for e in list(other_book[order.instrument]):

            if order.qty == 0:
                break
            if e.price == order.price:
                print(f"Matching order found: {e} and {order}")
                if e.qty> order.qty:
                    e.qty -=order.qty

                    executed_order = Trade(
                        seller_name=e.trader_name if order.side=="BUY" else order.trader_name,
                        buyer_name=order.trader_name if order.side=="BUY" else e.trader_name,
                        instrument=order.instrument,
                        qty=order.qty,
                        price=order.price
                    )
                    
                    order.qty = 0

                    executed.append(executed_order)

                    print(f"Order {order.order_id} fully matched and removed from book.")
                    break
                else:
                    order.qty -= e.qty

                    executed_order = Trade(
                        seller_name=e.trader_name if order.side=="BUY" else order.trader_name,
                        buyer_name=order.trader_name if order.side=="BUY" else e.trader_name,
                        instrument=order.instrument,
                        qty=e.qty,
                        price=order.price
                    )
                    e.qty = 0

                    executed.append(executed_order)

                    print(f"Order {e.order_id} fully matched and removed from book.")
                    other_book[order.instrument].remove(e)
                    self.orders_by_id.pop(e.order_id, None)
                    continue

        if order.qty > 0:
            book[order.instrument].append(order)
            self.orders_by_id[order.order_id] = order

            # filled = tmp - order.qty

            # if filled > 0:
            #     order_executed = copy.copy(order)
            #     order_executed.qty = filled
            #     executed.append(order_executed)
            return executed
        else:
            # order_executed = copy.copy(order)
            # order_executed.qty = tmp
            # executed.append(order_executed)
            return executed


# ---------------------------------------------------------------------------
# Framing (Lecture 2.1-2.2)
# ---------------------------------------------------------------------------

# def read_lines(conn):
#     """
#     Generator that yields complete, decoded, newline-stripped messages
#     from a connected TCP socket. Correctly handles:
#       - one message split across multiple recv() calls
#       - multiple messages delivered in a single recv() call
#       - a message split at an arbitrary byte position
#     """
#     buffer = b""
#     while True:
#         while b"\n" in buffer:
#             line, buffer = buffer.split(b"\n", 1)
#             yield line.decode(errors="replace")

#         data = conn.recv(4096)
#         if not data:
#             return  # peer closed the connection (orderly close) -- see Day 3
#         buffer += data


# ---------------------------------------------------------------------------
# Parsing (Lecture 2.3)
# ---------------------------------------------------------------------------

def parse_message(line):
    parts = line.strip().split()
    if not parts:
        return None, []
    command = parts[0].upper()
    args = parts[1:]
    return command, args


def parse_order_args(args):
    """Returns (instrument, qty, price) or raises ValueError with a reason."""
    if len(args) != 3:
        raise ValueError("expected 3 arguments: instrument quantity price")
    instrument, qty_str, price_str = args
    if instrument not in VALID_INSTRUMENTS:
        raise ValueError(f"unknown instrument {instrument}")
    if not qty_str.isdigit() or not price_str.isdigit():
        raise ValueError("quantity and price must be positive integers")
    qty, price = int(qty_str), int(price_str)
    if qty <= 0 or price <= 0:
        raise ValueError("quantity and price must be positive")
    return instrument, qty, price


# ---------------------------------------------------------------------------
# Per-client object (this is the "one object per user" piece you asked for)
# ---------------------------------------------------------------------------

class ClientSession:
    """
    Represents one connected client's state and behavior.
    A new ClientSession is created for every accepted connection.
    """

    def __init__(self, conn, addr, order_book: OrderBook, kq):
        # commons 
        self.conn = conn
        self.addr = addr
        self.order_book = order_book
        self.kq = kq
        self.out_buf = bytearray()  # for outgoing data to be sent
        self.in_buf = bytearray()
        self.client_type = None  # "TRADER" or "MARKET_DATA" -- set after LOGIN

        # for trader clients only
        self.username = None
        self.logged_in = False
        self.should_close = False

        # for market data clients only
        self.subscriptions = set()
        
        # as the non-blocking is now on, data may come in chunks, so we need to buffer it until we have complete lines to execute.


        self.handlers = {
            "LOGIN": self.handle_login,
            "BUY": self.handle_buy,
            "SELL": self.handle_sell,
            "CANCEL": self.handle_cancel,
            "QUIT": self.handle_quit,
            "SUBSCRIBE": self.handle_subscribe,
            "UNSUBSCRIBE": self.handle_unsubscribe,
        }

    def send(self, text: str):
        payload=(text + "\n").encode()
        try:
            sent_bytes=self.conn.send(payload)
        except BlockingIOError:
            sent_bytes=0
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.should_close = True            # reaped on its next loop pass
            return
        unsent_bytes=len(payload)-sent_bytes
        if unsent_bytes>0:
            print(f"Warning: {unsent_bytes} bytes unsent to {self.addr}, buffering for later")
            self.out_buf.extend(payload[sent_bytes:])
            write_ev= select.kevent(self.conn.fileno(), 
                                    filter=select.KQ_FILTER_WRITE, 
                                        flags=select.KQ_EV_ADD)
            self.kq.control([write_ev], 0)

    def run(self, inform):
        data=b""
        closed=False
        while True:
            try:
                chunk = self.conn.recv(4096)
            except BlockingIOError:
                break          # spurious wakeup, nothing to read, keep the session
            except (ConnectionResetError, OSError):
                return False         # hard error, tear down
            if chunk==b"":
                closed=True
                break
            data+=chunk

        self.in_buf += data
        while b"\n" in self.in_buf:
            line, self.in_buf = self.in_buf.split(b"\n", 1)
            try:
                self.dispatch(line.decode(errors="replace"))
                if len(executed_orders) > 0:
                    inform(executed_orders)
            except Exception as e:
                print(f"Exception while handling {self.addr}: {e}")
            if self.should_close: 
                return False
        return not closed 


        # try:
        #     print(f"data incoming from {self.addr}")
        #     for line in read_lines(self.conn):
        #         self.dispatch(line)
        # except Exception as e:
        #     print(f"Exception while handling {self.addr}: {e} \nSo closing the connection.")
        #     # gracefully close the connection on any unexpected error
        #     self.conn.close()
        #     return False
        # return True


    def dispatch(self, line):
        command, args = parse_message(line)
        if command is None:
            return  # blank line, ignore
        handler = self.handlers.get(command)
        if handler is None:
            self.send(f"ERROR unknown command {command}")
            return
        handler(args)

    # ---- individual command handlers ----

    def handle_login(self, args):
        if(self.client_type=="MARKET_DATA"):
            self.send("ERROR type is MARKET_DATA, cant login")
            return
        if len(args) != 1:
            self.send("ERROR LOGIN requires exactly one username")
            return
        if not self.logged_in:

            self.username = args[0]
            self.client_type = "TRADER"  
            self.logged_in = True
            print(f"    {self.addr} logged in as {self.username!r}")
            self.send("OK")
        else: 
            self.send("ERROR already logged in")

    def _require_login(self):
        if not self.logged_in:
            self.send("ERROR you must LOGIN first")
            return False
        return True

    def handle_buy(self, args):
        if self.client_type=="MARKET_DATA":
            self.send("ERROR type is MARKET_DATA, cant place orders")
            return
        self._handle_order("BUY", args)

    def handle_sell(self, args):
        if self.client_type=="MARKET_DATA":
            self.send("ERROR type is MARKET_DATA, cant place orders")
            return
        self._handle_order("SELL", args)

    def _handle_order(self, side, args):
        if not self._require_login():
            return
        try:
            instrument, qty, price = parse_order_args(args)
        except ValueError as e:
            self.send(f"ERROR {e}")
            return

        order_id = self.order_book.new_order_id()
        order = Order(
            order_id=order_id,
            trader_name=self.username,
            side=side,
            instrument=instrument,
            qty=qty,
            price=price,
        )
        executed = self.order_book.insert_order(order)
        executed_orders.extend(executed)
        # NOTE: matching against the opposite side happens in a later
        # lecture (Day 3, §2.6) -- for now we only accept and store.
        self.send(f"ORDER_ACCEPTED {order_id}")

    def handle_cancel(self, args):
        if(self.client_type=="MARKET_DATA"):
            self.send("ERROR type is MARKET_DATA, cant cancel orders")
            return
        if not self._require_login():
            return
        if len(args) != 1 or not args[0].isdigit():
            self.send("ERROR CANCEL requires a numeric order_id")
            return
        order_id = int(args[0])
        
        order = self.order_book.orders_by_id.get(order_id)
        if order and order.trader_name != self.username:
            self.send(f"ERROR order {order_id} does not belong to you")
            return
        if order is None:
            self.send(f"ERROR no such order {order_id}")
            return

        if not self.order_book.remove_order(order):
            self.send(f"ERROR order {order_id} could not be removed from book, most likely not found")
            return
        del self.order_book.orders_by_id[order_id]
        self.send(f"ORDER_CANCELLED {order_id}")

    def handle_quit(self, args):
        self.send("OK")
        self.should_close = True

    def handle_subscribe(self, args):
        if(self.client_type=="TRADER"):
            self.send("ERROR type is TRADER, cant subscribe to market data")
            return
        if len(args) != 1:
            self.send("ERROR SUBSCRIBE requires exactly one instrument")
            return
        instrument = args[0]
        if instrument not in VALID_INSTRUMENTS:
            self.send(f"ERROR unknown instrument {instrument}")
            return
        self.subscriptions.add(instrument)
        self.send(f"OK")
        self.client_type= "MARKET_DATA"

    def handle_unsubscribe(self, args):
        if(self.client_type=="TRADER"):
            self.send("ERROR type is TRADER, cant unsubscribe from market data")
            return
        if len(args) != 1:
            self.send("ERROR UNSUBSCRIBE requires exactly one instrument")
            return
        instrument = args[0]
        if instrument not in VALID_INSTRUMENTS:
            self.send(f"ERROR unknown instrument {instrument}")
            return
        if instrument not in self.subscriptions:
            self.send(f"ERROR not subscribed to {instrument}")
            return
        self.subscriptions.discard(instrument)
        self.send(f"OK")
        self.client_type= "MARKET_DATA"




# ---------------------------------------------------------------------------
# Server bootstrap (Lecture 1.3)
# ---------------------------------------------------------------------------

def main():
    order_book = OrderBook()

    kq=select.kqueue() # create a kqueue obj

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen()
    server_sock.setblocking(False) 
    server_event = select.kevent(server_sock.fileno(), filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD)
    kq.control([server_event], 0)
    print(f"Exchange Server listening on {HOST}:{PORT} ...")

    # Single-connection loop for now: fully service one client,
    # then go back to accept() for the next one.
    # Multiple SIMULTANEOUS clients require threads/select -- Day 3.
    sessions={} # store the client sessions.
    def cleanup_session(fd):
        s = sessions.pop(fd, None)
        if s and s.conn:
            s.conn.close()

    def inform(orders):
        for trade in orders:
            for session in sessions.values():
                if session.client_type=="MARKET_DATA" and (trade.instrument in session.subscriptions):
                    session.send(f"TRADE {trade.instrument} {trade.qty} {trade.price}")
                if session.client_type=="TRADER" and (trade.seller_name==session.username):
                    session.send(f"SOLD {trade.instrument} {trade.qty} {trade.price}")
                if session.client_type=="TRADER" and (trade.buyer_name==session.username):
                    session.send(f"BOUGHT {trade.instrument} {trade.qty} {trade.price}")
        orders.clear() # clear the list after informing all clients
        # this works because we are not doing parellel processing, so no other thread.
            

    while True:
        events = kq.control(None, 64, None) # wait for events
        for event in events:
            if event.ident == server_sock.fileno():
                while True:
                    try:
                        conn, addr = server_sock.accept()
                    except BlockingIOError:
                        break
                    conn.setblocking(False)
                    print(f"Accepted connection from {addr}")
                    session = ClientSession(conn, addr, order_book, kq)
                    sessions[conn.fileno()]=session
                    client_event = select.kevent(conn.fileno(), filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD)
                    kq.control([client_event], 0)
            elif event.flags & select.KQ_EV_EOF:
                session = sessions.get(event.ident)
                if session:
                    session.run(inform) # process remaining
                    print(f"Client {session.addr} disconnected.")
                    cleanup_session(event.ident)
                    # The kernel automatically removes closed fds from kqueue
            elif event.filter == select.KQ_FILTER_READ: 
                # handle client events
                session=sessions.get(event.ident)
                if session:
                    if not session.run(inform):
                        cleanup_session(event.ident)

                    
                else: 
                    print(f"Warning: No session found for fd {event.ident}")
                    # should not happen, but if it does, let's delete the event
                    kq.control([select.kevent(event.ident, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_DELETE)], 0)

            elif event.filter == select.KQ_FILTER_WRITE:
                session= sessions.get(event.ident)
                if session: 
                    if session.out_buf:
                        # send the data in the output buffer
                        try:
                            sent_bytes=session.conn.send(session.out_buf)
                        except BlockingIOError:
                            sent_bytes=0
                        session.out_buf=session.out_buf[sent_bytes:]
                    if not session.out_buf:
                        # remove write event if buffer is empty
                        kq.control([select.kevent(session.conn.fileno(), filter=select.KQ_FILTER_WRITE, flags=select.KQ_EV_DELETE)], 0)



if __name__ == "__main__":
    main()