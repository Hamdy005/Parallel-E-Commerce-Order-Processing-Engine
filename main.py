import os
import random
import threading
import time
from models.order import Order
from pipeline.stages import pipeline
from producer_consumer.producer_consumer import consumer, producer

NUM_ORDERS    = 15
NUM_CONSUMERS = 5

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ITEMS_FILE = os.path.join(BASE_DIR, "producer_consumer", "items.txt")

def load_items():
    with open(ITEMS_FILE, "r") as file:
        return [line.strip() for line in file if line.strip()]

def run_sequential(items):
    for i in range(NUM_ORDERS):
        order = Order(
            id=f"ORD-{i+1:03d}",
            item=random.choice(items),
            quantity=random.randint(1, 3),
            price=random.randint(50, 50000),
        )
        try:
            completed = pipeline(order)
            print(f"[{completed.id}] COMPLETE – status: {completed.status}\n")
        except Exception as error:
            print(f"[{order.id}] FAILED – {error}\n")

def run_parallel():
    worker_times = {}
    consumers = []

    start_time = time.time()

    # Start consumer threads first
    for i in range(1, NUM_CONSUMERS + 1):
        t = threading.Thread(target=consumer, args=(i, worker_times))
        t.start()
        consumers.append(t)

    # Start producer thread
    p = threading.Thread(target=producer, args=(NUM_ORDERS, NUM_CONSUMERS))
    p.start()

    # Wait for everything to finish
    p.join()
    for thread in consumers:
        thread.join()

    total_time = time.time() - start_time
    throughput = NUM_ORDERS / total_time

    print("=" * 55)
    print("  Parallel Execution Time Report:")
    for worker_id, duration in sorted(worker_times.items()):
        print(f"   ✅ Worker {worker_id} completed in {duration:.4f}s")

    print("-" * 55)
    print(f"  Total Time: {total_time:.4f}s")
    print(f"  Throughput: {throughput:.2f} orders/second")
    print("=" * 55)

    return total_time, throughput

if __name__ == "__main__":
    print("=" * 55)
    print("  E-Commerce Order Processing System")
    print("  Pattern: Producer-Consumer + Pipeline")
    print("=" * 55 + "\n")

    items = load_items()

    while True:
        print("Options:")
        print("  1) Run Sequential")
        print("  2) Run Parallel")
        print("  3) Exit")
        choice = input("Select an option (1-3): ").strip()

        if choice == "1":
            sequential_start = time.time()
            run_sequential(items)
            sequential_time = time.time() - sequential_start
            sequential_throughput = NUM_ORDERS / sequential_time

            print("=" * 55)
            print("  Sequential Execution Time Report:")
            print("-" * 55)
            print(f"  Total Time: {sequential_time:.4f}s")
            print(f"  Throughput: {sequential_throughput:.2f} orders/second")
            print("=" * 55 + "\n")
        elif choice == "2":
            run_parallel()
            print("\n")
        elif choice == "3":
            print("  All orders processed. System shutdown.")
            print("=" * 55)
            break
        else:
            print("Invalid choice. Please select 1, 2, or 3.\n")
