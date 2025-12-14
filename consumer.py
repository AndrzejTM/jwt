# consumer.py

import csv
import time
import os
import random
import portalocker

# Nazwa pliku kolejki
QUEUE_FILE = 'queue.csv'
# Czas trwania pracy (symulacja rozmowy)
WORK_DURATION_SECONDS = 3  # s
# Interwał sprawdzania kolejki
CHECK_INTERVAL_SECONDS = 4  # s

# Unikatowy ID dla tego konsumenta
CONSUMER_ID = f"C-{os.getpid()}-{random.randint(1000, 9999)}"


def update_task_status(task_id, new_status):
    """
    Odczytuje, aktualizuje status zadania i zapisuje zmiany do pliku,
    używając blokady plikowej (portalocker) w celu zapewnienia atomowości.
    """
    rows = []
    task_found = False

    # Użycie blokady plikowej (lock) na pliku kolejki.
    # Tryb 'r+' pozwala na odczyt i zapis. Timeout zapobiega zawieszeniu.
    try:
        # Blokada jest utrzymywana przez cały krytyczny region: odczyt -> modyfikacja -> zapis
        with portalocker.Lock(QUEUE_FILE, 'r+', timeout=10) as f:

            # 1. Wczytanie wszystkich danych
            f.seek(0)
            reader = csv.reader(f)
            for row in reader:
                rows.append(row)

            # 2. Wyszukanie i Modyfikacja zadania
            for i, row in enumerate(rows):
                if i == 0:  # Pomijamy nagłówek
                    continue

                # Sprawdzamy, czy wiersz ma ID zadania i czy to jest to, którego szukamy
                if row and len(row) > 2 and row[0] == task_id:
                    old_status = row[2]

                    # Zabezpieczenie: jeśli próbujemy zmienić na 'in progress', status musi być 'pending'
                    if new_status == 'in progress' and old_status != 'pending':
                        return False  # Zadanie już zostało pobrane przez innego konsumenta

                    # Aktualizacja statusu
                    rows[i][2] = new_status

                    # Ustawienie ID konsumenta tylko przy statusie in_progress
                    if new_status == 'in progress':
                        rows[i][3] = CONSUMER_ID

                    print(
                        f"[{CONSUMER_ID}] Zmieniono status zadania {task_id[:8]}... z '{old_status}' na '{new_status}'.")
                    task_found = True
                    break

            if not task_found:
                return True

            # 3. Zapis wszystkich danych (nadpisanie pliku)
            f.seek(0)  # Przechodzimy na początek pliku przed zapisem
            writer = csv.writer(f)
            writer.writerows(rows)
            f.truncate()  # Usuwa resztę pliku, jeśli nowy plik jest krótszy

        # Blokada zwolniona (plik zamknięty)
        return True

    except portalocker.LockException:
        print(f"[{CONSUMER_ID}] Oczekiwanie na blokadę przekroczyło limit (timeout). Inny konsument wykonuje zapis.")
        return False
    except FileNotFoundError:
        print(f"Plik kolejki {QUEUE_FILE} nie istnieje. Uruchom producenta.")
        return False
    except Exception as e:
        print(f"[{CONSUMER_ID}] Wystąpił błąd: {e}")
        return False


def get_pending_task():
    """Wyszukuje i próbuje 'skonsumować' pierwsze zadanie ze statusem 'pending'."""

    try:
        with open(QUEUE_FILE, 'r', newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:  # Pomijamy nagłówek
                    continue

                # Sprawdzamy, czy wiersz ma odpowiedni status
                if row and len(row) > 2 and row[2] == 'pending':
                    task_id = row[0]

                    # 2. Próba atomowej aktualizacji statusu (pobranie zadania)
                    # To wywołanie użyje blokady, by bezpiecznie zmienić 'pending' na 'in progress'.
                    if update_task_status(task_id, 'in progress'):
                        print(f"[{CONSUMER_ID}] POBRANO zadanie: {task_id[:8]}...")
                        return task_id

                    return None  # Zadanie pobrane przez innego konsumenta lub błąd blokady

        return None  # Brak zadań do wykonania

    except FileNotFoundError:
        return None


def process_task(task_id):
    """Wykonanie każdej pracy trwa 30s."""
    print(f"[{CONSUMER_ID}] Rozpoczynam pracę nad zadaniem {task_id[:8]}... (Trwa: {WORK_DURATION_SECONDS}s)")
    time.sleep(WORK_DURATION_SECONDS)  # Symulacja pracy
    print(f"[{CONSUMER_ID}] ZAKOŃCZONO pracę nad zadaniem {task_id[:8]}...")

    # Ustawienie statusu na 'done' (również chronione blokadą)
    update_task_status(task_id, 'done')


def consumer_loop():
    """Główna pętla konsumenta."""
    print(f"--- CONSUMER START: {CONSUMER_ID} ---")

    # Pętla stałego uruchomienia
    while True:
        task_id = get_pending_task()

        if task_id:
            process_task(task_id)
        else:
            print(f"[{CONSUMER_ID}] Brak zadań w kolejce. Oczekiwanie {CHECK_INTERVAL_SECONDS}s...")

        time.sleep(CHECK_INTERVAL_SECONDS)  # Odczytywanie pliku co 5s


if __name__ == '__main__':
    consumer_loop()