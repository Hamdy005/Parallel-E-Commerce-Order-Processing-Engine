import os
import random
import sys
import time
from models.order import Order
from multiprocessing_system.process_manager import run_parallel_processes
from pipeline.stages import pipeline
from threading_lock.inventory import inventory

NUM_ORDERS    = 15
NUM_CONSUMERS = 5

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ITEMS_FILE = os.path.join(BASE_DIR, "producer_consumer", "items.txt")

def load_items():
    with open(ITEMS_FILE, "r") as file:
        return [line.strip() for line in file if line.strip()]

def run_sequential(items):
    inventory.reset()   # restore stock to full before each run
    for i in range(NUM_ORDERS):
        order = Order(
            id=f"ORD-{i+1:03d}",
            item=random.choice(items),
            quantity=random.randint(1, 3),
            price=random.randint(5000, 20000),
        )
        try:
            completed = pipeline(order)
            if os.environ.get("SHOW_TRACKING", "1") == "1":
                print(f"[{completed.id}] COMPLETE - status: {completed.status}\n")
        except Exception as error:
            if os.environ.get("SHOW_TRACKING", "1") == "1":
                print(f"[{order.id}] FAILED - {error}\n")

def run_parallel():
    inventory.reset()   # restore stock to full before each run
    return run_parallel_processes(NUM_ORDERS, NUM_CONSUMERS)

def print_time_report(par_time, par_tp, par_workers, par_cores, sequential=False):
    if sequential:
        print("=" * 55)
        print("  Sequential Execution Time Report:")
        print("-" * 55)
        print(f"  Total Time: {par_time:.4f}s")
        print(f"  Throughput: {par_tp:.2f} orders/second")
        print("=" * 55 + "\n")
    else:
        print("=" * 55)
        print("  Parallel Execution Time Report (Multiprocessing):")
        for worker_id, duration in sorted(par_workers.items()):
            print(f"   [>>] Worker {worker_id} completed in {duration:.4f}s")
        print("-" * 55)
        print(f"  Logical Cores used  : {par_cores}")
        print(f"  Total Time  : {par_time:.4f}s")
        print(f"  Throughput  : {par_tp:.2f} orders/second")
        print("=" * 55 + "\n")


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
        print("  3) Run Parallel with Lock Tracking")
        print("  4) Exit")
        choice = input("Select an option (1-4): ").strip()

        if choice == "1":
            os.environ["SHOW_TRACKING"] = "1"
            os.environ["SHOW_LOCK_TRACKING"] = "0"
            sequential_start = time.time()
            run_sequential(items)
            sequential_time = time.time() - sequential_start
            sequential_throughput = NUM_ORDERS / sequential_time
            print_time_report(sequential_time, sequential_throughput, {}, 1, sequential=True)
            
        elif choice == "2":
            os.environ["SHOW_TRACKING"] = "1"
            os.environ["SHOW_LOCK_TRACKING"] = "0"
            par_time, par_tp, par_workers, par_cores = run_parallel()
            print_time_report(par_time, par_tp, par_workers, par_cores)

        elif choice == "3":
            os.environ["SHOW_TRACKING"] = "1"
            os.environ["SHOW_LOCK_TRACKING"] = "1"
            par_time, par_tp, par_workers, par_cores = run_parallel()
            print_time_report(par_time, par_tp, par_workers, par_cores)

        elif choice == "4":
            print("  All orders processed. System shutdown.")
            print("=" * 55)
            break
        else:
            print("Invalid choice. Please select 1, 2, 3, or 4.\n")