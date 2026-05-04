import os
import sys

# Get the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)

if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)

from models.order import Order
import random
import time
from multiprocessing import Queue 
from pipeline.stages import pipeline

# Defining Queue
queue = Queue(maxsize = 100)

# Defining Items
ITEMS = []
ITEMS_FILE = os.path.join(SCRIPT_DIR, 'items.txt')

with open(ITEMS_FILE, 'r') as file:
    for item in file.readlines():
        ITEMS.append(item.strip())


# Producer Function
def producer(number_of_orders = 20, number_of_consumers = 1):
    
    for i in range(number_of_orders):
        order = Order(
            id=f"ORD-{i+1:03d}",
            item=random.choice(ITEMS),
            quantity=random.randint(1, 3),
            price=random.randint(50, 50000),
        )
        queue.put(order)
        time.sleep(0.1)

    for i in range(number_of_consumers):
        queue.put(None)


# Consumer Function
def consumer(worker_id, worker_times=None):
    if worker_times is not None:
        print(f"   ⏳ Worker {worker_id} started...")
        start_time = time.time()

    while True:
        # Getting Current Order
        order = queue.get()
        if order is None:
            break 
        
        # Processing Current Order
        try:
            completed = pipeline(order)
            print(f"[{completed.id}] COMPLETE – status: {completed.status}\n")
        
        except Exception as error:
            print(f"[{order.id}] FAILED – {error}\n")
     
    if worker_times is not None:
        worker_times[worker_id] = time.time() - start_time