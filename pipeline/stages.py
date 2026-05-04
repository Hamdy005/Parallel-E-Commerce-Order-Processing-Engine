import random 
import time

def validate_order(order):
    """Stage 1 – make sure required fields exist and values are sane."""
    time.sleep(0.1)
    if not order.item or order.quantity <= 0:
        raise ValueError(f"Order {order.id} is invalid")
    else:
        order.status = 'validated'
    return order

def process_payment(order):
    """Stage 2 – charge the customer."""
    time.sleep(0.2)
    if random.random() < 0.1:   # 10% Payment Failure
        raise RuntimeError(f"Payment failed for order {order.id}")
    else:
        order.status = 'paid'
    return order
    
def update_inventory(order):
    """Stage 3 – deduct stock."""
    time.sleep(0.1)
    order.status = 'inventory_updated'
    return order

def ship_order(order):
    """Stage 4 – dispatch to courier."""
    time.sleep(0.15)
    order.status = 'shipped'
    return order


def pipeline(order):
    order = validate_order(order)
    order = process_payment(order)
    order = update_inventory(order)
    order = ship_order(order)
    return order

