# server.py
import socket
import threading
import sqlite3
from database import init_db, upsert_user, set_offline, insert_message,DB_FILE, db_lock

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 5000
BUFFER_SIZE = 4096
PREDEFINED_QUERIES = {
    1: "SELECT username FROM connections WHERE is_online = 1;",
    2: "SELECT COUNT(*) AS total_users FROM connections;",
    3: """SELECT m.id, m.content, m.timestamp
          FROM messages AS m
          JOIN connections AS c ON m.sender_id = c.id
          WHERE c.username = ?;""",
    4: """SELECT *
          FROM messages
          WHERE ',' || receiver_ids || ',' LIKE '%,' || (
              SELECT id FROM connections WHERE username = ?
          ) || ',%';""",
    5: "SELECT COUNT(*) AS total_messages FROM messages;",
    6: """SELECT c1.username AS sender, c2.username AS receiver, COUNT(*) AS message_count
          FROM messages AS m
          JOIN connections AS c1 ON m.sender_id = c1.id
          JOIN connections AS c2 ON ',' || m.receiver_ids || ',' LIKE '%,' || c2.id || ',%'
          GROUP BY c1.username, c2.username;""",
    7: """SELECT *
          FROM messages
          WHERE sender_id = (SELECT id FROM connections WHERE username = ?)
             OR ',' || receiver_ids || ',' LIKE '%,' || (
                 SELECT id FROM connections WHERE username = ?
             ) || ',%';"""
}

clients = [] 
clients_lock = threading.Lock()


def broadcast(message, sender=None, except_sock=None):
    with clients_lock:
        to_remove = []
        for sock, user in clients:
            if sock == except_sock:
                continue
            try:
                sock.sendall(f"{sender if sender else ''}{message}".encode())
            except:
                to_remove.append((sock, user))
        for r in to_remove:
            clients.remove(r)
            r[0].close()


def handle_client(sock):
    sock.send(b"Enter username: ")
    username = sock.recv(1024).decode().strip() or "Unknown"
    upsert_user(username, *sock.getpeername())
    with clients_lock:
        clients.append((sock, username))
        online_users = [u for _, u in clients]
    insert_message('Server', f"{username} joined", online_users)
    broadcast(f"{username} joined!\n", sender="Server: ")

    try:
        while True:
            data = sock.recv(BUFFER_SIZE)
            if not data:
                break
            text = data.decode(errors='ignore').strip()

            if text.lower() == 'exit':
                broadcast(f"{username} left.\n", sender="Server: ", except_sock=sock)
                set_offline(username)
                with clients_lock:
                    clients[:] = [(s, u) for s, u in clients if s != sock]
                    online_users = [u for _, u in clients]
                insert_message('Server', f"{username} left", online_users)
                break

            with clients_lock:
                online_users = [u for _, u in clients]
            insert_message(username, text, online_users)

            if text.startswith('/pm '):
                _, target, msg = text.split(' ', 2)
                sent = False
                with clients_lock:
                    for s, u in clients:
                        if u == target:
                            s.sendall(f"[PM] {username}: {msg}\n".encode())
                            sent = True
                            break
                if not sent:
                    sock.sendall(f"Server: User '{target}' not found.\n".encode())

            # elif text.startswith('/file '):
            #     _, target, filename = text.split(' ', 2)
            #     sock.sendall(b"Server: Send file data.\n")
            #     file_data = b''
            #     while True:
            #         chunk = sock.recv(BUFFER_SIZE)
            #         file_data += chunk
            #         if not chunk or len(chunk) < BUFFER_SIZE:
            #             break
            #     sent = False
            #     with clients_lock:
            #         for s, u in clients:
            #             if u == target:
            #                 s.sendall(f"/file {username} {filename}\n".encode())
            #                 s.sendall(file_data)
            #                 sent = True
            #                 break
            elif text.startswith('/file '):
                _, target, filename = text.split(' ', 2)

                sock.sendall(b"READYFILE")

                file_data = b''
                while True:
                    chunk = sock.recv(BUFFER_SIZE)
                    if not chunk:
                        break
                    file_data += chunk
                    if len(chunk) < BUFFER_SIZE:
                        break

                with open(filename, 'wb') as f:
                    f.write(file_data)
                print(f"[Server] Saved file as ./{filename}")

                sent = False
                with clients_lock:
                    for s, u in clients:
                        if u == target:
                            s.sendall(f"/file {username} {filename}\n".encode())
                            s.sendall(file_data)
                            sent = True
                            print(f"[Server] Forwarded '{filename}' from {username} to {target}")
                            break
                if not sent:
                    sock.sendall(f"Server: User '{target}' not found.\n".encode())

            elif text.startswith('/query '):
                parts = text.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    qid = int(parts[1])
                    sql = PREDEFINED_QUERIES.get(qid)
                    if sql:
                        params = ()
                        if '?' in sql:
                            if len(parts) >= 3:
                                params = (parts[2],) if qid in (3,4) else (parts[2], parts[2])
                            else:
                                sock.sendall(b"Server: missing username parameter.\n")
                                continue

                        with db_lock, sqlite3.connect(DB_FILE) as conn:
                            cur = conn.cursor()
                            cur.execute(sql, params)
                            rows = cur.fetchall()

                        if rows:
                            for row in rows:
                                line = ' | '.join(str(x) for x in row) + '\n'
                                sock.sendall(line.encode())
                        else:
                            sock.sendall(b"<no rows>\n")
                    else:
                         sock.sendall(f"Server: unknown query id {qid}\n".encode())
                else:
                    sock.sendall(b"Server: invalid query command.\n")

            else:
                broadcast(f"{username}: {text}\n")

    finally:
        sock.close()

def main():
    init_db()
    server = socket.socket()
    server.bind((SERVER_HOST, SERVER_PORT))
    server.listen(5)
    print(f"Listening on {SERVER_HOST}:{SERVER_PORT}...")
    try:
        while True:
            client_sock, _ = server.accept()
            threading.Thread(target=handle_client, args=(client_sock,), daemon=True).start()
    except KeyboardInterrupt:
        broadcast("Chat closed.\n", sender="Server: ")
        server.close()


if __name__ == '__main__':
    main()
