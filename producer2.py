# producer.py

import sqlite3
import time
import uuid
import os

# Nazwa pliku bazy danych
DB_FILE = 'queue.db'
# Liczba zadań do wygenerowania
NUM_TASKS = 100


def get_db_connection():
    """Tworzy i zwraca połączenie z bazą danych."""
    # SQLite jest wbudowany w Pythona.
    conn = sqlite3.connect(DB_FILE)
    return conn


def setup_database(conn):
    """Tworzy tabelę zadań, jeśli nie istnieje."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            timestamp INTEGER,
            status TEXT,
            consumer_id TEXT
        )
    ''')
    conn.commit()
    print(f"Baza danych {DB_FILE} i tabela 'tasks' gotowe.")


def generate_tasks(conn, num_tasks):
    """Generuje i zapisuje zadania do bazy danych."""
    cursor = conn.cursor()
    tasks = []
    current_time = int(time.time())

    # Utwórz listę nowych zadań
    for _ in range(num_tasks):
        task_id = str(uuid.uuid4())
        # status 'pending' - praca czeka na konsumera
        # Kolumny: id, timestamp, status, consumer_id
        tasks.append((task_id, current_time, 'pending', ''))

        # Wstaw zadania w jednej transakcji
    cursor.executemany(
        "INSERT INTO tasks (id, timestamp, status, consumer_id) VALUES (?, ?, ?, ?)",
        tasks
    )
    conn.commit()

    print(f"Zapisano {num_tasks} nowych zadań do kolejki SQLite.")


if __name__ == '__main__':
    print("--- PRODUCER START (SQLite) ---")

    conn = None
    try:
        conn = get_db_connection()
        setup_database(conn)
        generate_tasks(conn, NUM_TASKS)  # 100 zadań

    except sqlite3.Error as e:
        print(f"Wystąpił błąd SQLite: {e}")
    finally:
        if conn:
            conn.close()

    print("--- PRODUCER END ---")