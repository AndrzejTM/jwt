# consumer.py

import sqlite3
import time
import os
import random

# Nazwa pliku bazy danych
DB_FILE = 'queue.db'
# Czas trwania pracy (symulacja rozmowy)
WORK_DURATION_SECONDS = 30  # s
# Interwał sprawdzania kolejki (co 5s)
CHECK_INTERVAL_SECONDS = 5  # s

# Unikatowy ID dla tego konsumenta
CONSUMER_ID = f"C-{os.getpid()}-{random.randint(1000, 9999)}"


def get_db_connection():
    """Tworzy i zwraca połączenie z bazą danych."""
    # Użycie izolacji w trybie DEFERRED jest wystarczające, ponieważ SQLite domyślnie blokuje
    # bazę na czas transakcji, zapewniając atomowość.
    conn = sqlite3.connect(DB_FILE, isolation_level='DEFERRED')
    return conn


def get_and_lock_task(conn):
    """
    Pobiera jedno zadanie ze statusem 'pending' i natychmiast zmienia jego status na
    'in progress' w jednej, atomowej transakcji.
    """
    cursor = conn.cursor()
    task_id = None

    # Rozpoczęcie transakcji
    try:
        # 1. Znajdź jedno zadanie 'pending'. Używamy LIMIT 1.
        cursor.execute(
            "SELECT id FROM tasks WHERE status = 'pending' ORDER BY timestamp ASC LIMIT 1"
        )
        row = cursor.fetchone()

        if row is None:
            return None  # Brak zadań

        task_id = row[0]

        # 2. Zmień status zadania na 'in progress'
        cursor.execute(
            "UPDATE tasks SET status = ?, consumer_id = ? WHERE id = ?",
            ('in progress', CONSUMER_ID, task_id)
        )

        # 3. Zatwierdź transakcję. To zwalnia blokadę i zapewnia, że status jest zmieniony.
        conn.commit()

        print(f"[{CONSUMER_ID}] POBRANO i ZABLOKOWANO zadanie: {task_id[:8]}...")
        return task_id

    except sqlite3.OperationalError as e:
        # Występuje, gdy baza jest zablokowana przez innego konsumenta (jest to normalne)
        conn.rollback()
        print(f"[{CONSUMER_ID}] Baza danych chwilowo zablokowana przez inny proces. Oczekiwanie...")
        return None
    except Exception as e:
        conn.rollback()
        print(f"[{CONSUMER_ID}] Wystąpił nieoczekiwany błąd podczas pobierania zadania: {e}")
        return None


def complete_task(conn, task_id):
    """Ustawia status zadania na 'done'."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE tasks SET status = ? WHERE id = ?",
            ('done', task_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"[{CONSUMER_ID}] Błąd przy ustawianiu statusu 'done' dla {task_id[:8]}...: {e}")


def process_task(conn, task_id):
    """Symuluje wykonywanie pracy przez określony czas."""
    print(f"[{CONSUMER_ID}] Rozpoczynam pracę nad zadaniem {task_id[:8]}... (Trwa: {WORK_DURATION_SECONDS}s)")
    time.sleep(WORK_DURATION_SECONDS)  # Symulacja pracy (30s)
    print(f"[{CONSUMER_ID}] ZAKOŃCZONO pracę nad zadaniem {task_id[:8]}...")

    # 3. Ustawienie statusu na 'done'
    complete_task(conn, task_id)


def consumer_loop():
    """Główna pętla konsumenta."""
    print(f"--- CONSUMER START: {CONSUMER_ID} (SQLite) ---")

    conn = None
    try:
        conn = get_db_connection()

        # Pętla stałego uruchomienia (while true)
        while True:
            task_id = get_and_lock_task(conn)

            if task_id:
                process_task(conn, task_id)
            else:
                print(f"[{CONSUMER_ID}] Brak zadań 'pending' w kolejce. Oczekiwanie {CHECK_INTERVAL_SECONDS}s...")

            time.sleep(CHECK_INTERVAL_SECONDS)  # Odczytywanie bazy co 5s

    except sqlite3.Error as e:
        print(f"Błąd połączenia z bazą danych: {e}")
    except KeyboardInterrupt:
        print("\nPrzerwanie pracy konsumenta.")
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    consumer_loop()