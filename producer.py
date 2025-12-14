# producer.py

import csv
import time
import uuid

# Nazwa pliku kolejki
QUEUE_FILE = 'queue.csv'
# Liczba zadań do wygenerowania
NUM_TASKS = 100


def create_initial_queue_file():
    """Tworzy plik CSV i zapisuje nagłówki, jeśli nie istnieje."""
    try:
        with open(QUEUE_FILE, 'x', newline='') as f:
            writer = csv.writer(f)
            # Kolumny: ID, Czas utworzenia, Status, ID Konsumenta (puste na początku)
            writer.writerow(['id', 'timestamp', 'status', 'consumer_id'])
        print(f"Utworzono plik {QUEUE_FILE} z nagłówkami.")
    except FileExistsError:
        # Plik już istnieje, użyjemy go.
        pass


def generate_tasks(num_tasks):
    """Generuje i zapisuje zadania do pliku kolejki."""
    tasks = []
    current_time = int(time.time())

    # Utwórz listę nowych zadań
    for i in range(1, num_tasks + 1):
        task_id = str(uuid.uuid4())
        # status 'pending' - praca czeka na konsumera
        tasks.append([task_id, current_time, 'pending', ''])

        # Zapisz zadania do pliku
    with open(QUEUE_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(tasks)

    print(f"Zapisano {num_tasks} nowych zadań do kolejki.")


if __name__ == '__main__':
    print("--- PRODUCER START ---")
    create_initial_queue_file()
    generate_tasks(NUM_TASKS)  # 100 zadań
    print("--- PRODUCER END ---")