## slide 4
from contextlib import closing, contextmanager
import sqlite3
from threading import Lock


class Stock(object):
    """Represents a stock holding (symbol, quantity, and price"""
    def __init__(self, symbol='', quantity=0, price=0.0):
        self.symbol = symbol
        self.quantity = quantity
        self.price = price

    @classmethod
    def from_row(cls, row):
        return Stock(*row)


class StockDB(object):
    def __init__(self):
        ## 1. ADD check_same_thread... otherwise Python will complain.
        ## This allows us to use multiple threads on the same connection. Be sure to have sqlite built with
        ## the Serialized option (default) and version 3.3.1 or later.
        ## 2. Change the isolation level to deferred so we can control transactions
        self._connection = sqlite3.connect('example.db', check_same_thread=False, isolation_level='DEFERRED')
        ## WAL requires SQLite version 3.7 or later.
        self._connection.execute('PRAGMA journal_mode = WAL')
        self._lock = Lock()

    def create_table(self):
        """Create the stocks table"""
        with closing(self._connection.cursor()) as cursor:
            cursor.execute("CREATE TABLE stocks (symbol text, quantity real, price real)")

    # This is vulnerable to injection. DO NOT execute statements where the string is built from user input
    # def insert(self, stock):
    #     """Insert stock in DB"""
    #     keys = stock.__dict__.iterkeys()
    #     values = (sql_value(x) for x in stock.__dict__.itervalues())
    #     with closing(self._connection.cursor()) as cursor:
    #         cursor.execute("INSERT INTO stocks({}) VALUES ({})".format(", ".join(keys), ", ".join(values)))

    def insert(self, stock):
        """Insert stock in DB. stock cannot already be in the database"""
        ## Note this is using prepared statement format so it is safe from injection
        places = ','.join(['?'] * len(stock.__dict__))
        keys = ','.join(stock.__dict__.iterkeys())
        values = tuple(stock.__dict__.itervalues())
        with closing(self._connection.cursor()) as cursor:
            cursor.execute("INSERT INTO stocks({}) VALUES ({})".format(keys, places), values)

    def lookup(self, symbol):
        """Return stock if found, else None"""
        with closing(self._connection.cursor()) as cursor:
            cursor.execute('SELECT * FROM stocks WHERE symbol = ?', (symbol,))
            row = cursor.fetchone()
            if row:
                return Stock.from_row(row)

    def update(self, stock):
        """Update an existing stock"""
        updates = ','.join(key + ' = ?' for key in stock.__dict__.iterkeys())
        values = tuple(stock.__dict__.values() + [stock.symbol])
        with closing(self._connection.cursor()) as cursor:
            cursor.execute('UPDATE stocks SET {} WHERE symbol = ?'.format(updates), values)

    # This is ok if each thread has its own connection. The writes will be serialized by SQLite
    # I recommend sharing connections (below) because it it makes bookkeeping easier. If you do go with a separate
    # connection per thread, consider overriding Thread.Run so you close the connection when Run completes.
    # Otherwise, your unittests can have a hard time creating and deleting lots of temporary databases.
    # def transaction(self):
    #     return self._connection

    # This allows threads to share connections. When multiple threads are writing we perform the serialization
    # by holding self._lock. No performance penalty here from our lock because SQLite only allows one writer at time.
    @contextmanager
    def transaction(self):
        with self._lock:
            try:
                yield
                self._connection.commit()
            except:
                self._connection.rollback()
                raise

# Some sample usage
def main():
    db = StockDB()
    db.create_table()
    stock = Stock('GOOG', 5, 600.10)
    with db.transaction():
        db.insert(stock)
    stock = db.lookup('GOOG')
    stock.quantity += 100
    with db.transaction():
        db.update(stock)
    stock.quantity += 100
    stock.price = 550.50
    with db.transaction():
        db.update(stock)

if __name__ == "__main__":
    main()


