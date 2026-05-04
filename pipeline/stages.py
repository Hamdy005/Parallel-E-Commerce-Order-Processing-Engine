import time

from threading_lock.inventory import inventory          
from worker_pool.payment_pool import payment_pool      


def validate_order(order):
    """Stage 1 – make sure required fields exist and values are sane."""
    time.sleep(0.1)
    if not order.item or order.quantity <= 0:
        raise ValueError(f"Order {order.id} is invalid")
    order.status = "validated"
    return order


def process_payment(order):
    """
    Stage 2 – charge the customer via the worker-pool payment processor.

    Delegates to PaymentPool.process(), which runs the charge inside a
    ThreadPoolExecutor thread — enabling true parallel payment processing
    when multiple pipeline calls happen concurrently.
    """
    return payment_pool.process(order)   # raises RuntimeError on failure


def update_inventory(order):
    """
    Stage 3 – deduct stock using the thread-safe InventoryManager.

    The InventoryManager's internal Lock ensures that concurrent threads
    cannot cause a race condition on shared stock levels.
    """
    inventory.deduct_stock(order)        # raises RuntimeError if out-of-stock
    order.status = "inventory_updated"
    return order


def ship_order(order):
    """Stage 4 – dispatch to courier."""
    time.sleep(0.15)
    order.status = "shipped"
    return order


def pipeline(order):
    """Run all four stages in sequence for a single order."""
    order = validate_order(order)
    order = process_payment(order)
    order = update_inventory(order)
    order = ship_order(order)
    return order