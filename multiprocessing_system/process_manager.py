import multiprocessing
import time
from producer_consumer.producer_consumer import consumer, producer

def run_parallel_processes(num_orders, num_consumers):
    """
    Run the producer-consumer flow using multiprocessing for true parallel execution.
    This replaces threading to distribute the workload across CPU cores.
    """
    # Use a Manager dict so worker times can be shared across processes
    manager = multiprocessing.Manager()
    worker_times = manager.dict()
    shared_queue = manager.Queue(maxsize=100)
    consumers = []

    start_time = time.time()

    # Start consumer processes first
    for i in range(1, num_consumers + 1):
        p = multiprocessing.Process(target=consumer, args=(i, worker_times, shared_queue))
        p.start()
        consumers.append(p)

    # Start producer process
    prod_p = multiprocessing.Process(target=producer, args=(num_orders, num_consumers, shared_queue))
    prod_p.start()

    # Wait for everything to finish
    prod_p.join()
    for p in consumers:
        p.join()

    total_time = time.time() - start_time
    throughput = num_orders / total_time

    print("=" * 55)
    print("  Parallel Execution Time Report (Multiprocessing):")
    # Convert Manager dict to standard dict for sorting
    for worker_id, duration in sorted(dict(worker_times).items()):
        print(f"   [>>] Worker {worker_id} completed in {duration:.4f}s")

    print("-" * 55)
    print(f"  Total Time: {total_time:.4f}s")
    print(f"  Throughput: {throughput:.2f} orders/second")
    print("=" * 55)

    return total_time, throughput