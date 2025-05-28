# database.py
import sqlite3
import threading
from datetime import datetime

DB_FILE = 'chat_server.db'
db_lock = threading.Lock()

def init_db():
    """Initialize the database with connections and messages tables."""
    with db_lock, sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                ip_address TEXT,
                port INTEGER,
                connection_time TEXT,
                disconnection_time TEXT,
                is_online BOOLEAN
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                receiver_ids TEXT NOT NULL,
                FOREIGN KEY(sender_id) REFERENCES connections(id)
            )
        ''')
        conn.commit()


def upsert_user(username, ip, port):
    """Insert or update a user connection record, mark online."""
    now = datetime.now().isoformat(sep=' ', timespec='seconds')
    with db_lock, sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM connections WHERE username = ?", (username,))
        row = c.fetchone()
        if row:
            user_id = row[0]
            c.execute(
                """
                UPDATE connections
                SET ip_address=?, port=?, connection_time=?, is_online=1, disconnection_time=NULL
                WHERE id=?
                """,
                (ip, port, now, user_id)
            )
        else:
            c.execute(
                """
                INSERT INTO connections
                (username, ip_address, port, connection_time, is_online)
                VALUES (?, ?, ?, ?, 1)
                """,
                (username, ip, port, now)
            )
            user_id = c.lastrowid
        conn.commit()
        return user_id


def set_offline(username):
    """Mark a user offline and store disconnection time."""
    now = datetime.now().isoformat(sep=' ', timespec='seconds')
    with db_lock, sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE connections SET is_online=0, disconnection_time=? WHERE username=?",
            (now, username)
        )
        conn.commit()


def insert_message(sender, content, receivers):
    """Store a message with sender and list of online receivers."""
    timestamp = datetime.now().isoformat(sep=' ', timespec='seconds')
    with db_lock, sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM connections WHERE username=?", (sender,))
        sender_row = c.fetchone()
        if not sender_row:
            return
        sender_id = sender_row[0]
        receiver_ids = []
        for user in receivers:
            c.execute("SELECT id FROM connections WHERE username=?", (user,))
            row = c.fetchone()
            if row:
                receiver_ids.append(str(row[0]))
        receivers_str = ','.join(receiver_ids)
        c.execute(
            """
            INSERT INTO messages
            (sender_id, content, timestamp, receiver_ids)
            VALUES (?, ?, ?, ?)
            """,
            (sender_id, content, timestamp, receivers_str)
        )
        conn.commit()
