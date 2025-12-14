# producer.py

import csv
import time
import uuid
import os

# Nazwa pliku kolejki
QUEUE_FILE = 'queue.csv'
# Liczba zadań do wygenerowania
NUM_TASKS = 100


def create_initial_queue_file():
    """Tworzy plik CSV i zapisuje nagłówki, jeśli nie istnieje."""
    try:
        # Tryb 'x' - ekskluzywne tworzenie. Wyrzuca błąd, jeśli plik istnieje.
        with open(QUEUE_FILE, 'x', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Kolumny: ID, Czas utworzenia, Status, ID Konsumenta (puste na początku)
            writer.writerow(['id', 'timestamp', 'status', 'consumer_id'])
        print(f"Utworzono plik {QUEUE_FILE} z nagłówkami.")
    except FileExistsError:
        # Plik już istnieje, nie robimy nic.
        pass
    except Exception as e:
        print(f"Błąd podczas tworzenia pliku {QUEUE_FILE}: {e}")


def generate_tasks(num_tasks):
    """Generuje i zapisuje zadania do pliku kolejki."""

    # 1. Sprawdzenie, czy plik istnieje (w przeciwnym razie wywołuje stworzenie nagłówka)
    if not os.path.exists(QUEUE_FILE):
        create_initial_queue_file()

    tasks = []
    current_time = int(time.time())

    # Utwórz listę nowych zadań
    for i in range(1, num_tasks + 1):
        task_id = str(uuid.uuid4())
        # status 'pending' - praca czeka na konsumera
        tasks.append([task_id, current_time, 'pending', ''])

    try:
        # 2. Zapis zadania do pliku w trybie 'a' (append/dopisywanie)
        with open(QUEUE_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(tasks)

        print(f"Zapisano {num_tasks} nowych zadań do kolejki.")
    except PermissionError:
        print(
            f"BŁĄD: Brak uprawnień do zapisu w pliku {QUEUE_FILE}. Upewnij się, że nie jest otwarty przez inny program/konsumenta.")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd podczas zapisu: {e}")


if __name__ == '__main__':
    print("--- PRODUCER START ---")
    create_initial_queue_file()
    generate_tasks(NUM_TASKS)  # 100 zadań
    print("--- PRODUCER END ---")