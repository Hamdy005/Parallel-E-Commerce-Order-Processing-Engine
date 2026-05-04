import multiprocessing
import os
import time
from producer_consumer.producer_consumer import consumer, producer


def run_parallel_processes(num_orders, num_consumers, num_processes=None):
    """
    Manages the parallel execution of the E-Commerce Order Processing System.
    This function sets up the multiprocessing environment, creates shared resources,
    spawns producer and consumer processes, and tracks their execution times.
    """
    
    # Determine Core Allocation (Multiprocessing relies on separate CPU cores)
    resolved_cores = num_processes or os.cpu_count() or 1

    print(f"[Config] Using {resolved_cores} core(s) "
          f"({'auto-detected' if num_processes is None else 'user-specified'})")

    
    # Manager Allows Threading + Multiprocessing (Due to shared queue)
    manager = multiprocessing.Manager()
    worker_times = manager.dict()  
    shared_queue = manager.Queue(maxsize=100)
    consumers = []
    start_time = time.time()

    # Limit Consumers to Available Cores
    actual_consumers = min(num_consumers, resolved_cores)
    if actual_consumers < num_consumers:
        print(f"[Config] num_consumers={num_consumers} capped to {actual_consumers} "
              f"(core limit)")

    # Each consumer will pull orders from the shared_queue and process them.
    for i in range(1, actual_consumers + 1):
        p = multiprocessing.Process(
            target=consumer,
            args=(i, worker_times, shared_queue)
        )
        p.start() # Start the process asynchronously
        consumers.append(p)

    # The producer generates orders and pushes them into the shared_queue.
    prod_p = multiprocessing.Process(
        target=producer,
        args=(num_orders, actual_consumers, shared_queue)
    )
    prod_p.start()

    # Synchronization (Wait for Completion)
    prod_p.join()
    for p in consumers:
        p.join()

    total_time = time.time() - start_time
    throughput = num_orders / total_time
    return total_time, throughput, dict(worker_times), resolved_cores