"""
worker_pool/payment_pool.py  –  Dev B: Concurrency Engine
==========================================================
Parallel payment processing via a thread worker pool.

Problem solved:
    Processing payments one-by-one (sequentially) is slow.
    A real payment gateway takes ~0.2–0.5 s per transaction.
    With 15 orders that is 3–7 s of pure waiting.

Solution:
    A ThreadPoolExecutor acts as a *worker pool*: a fixed number of
    threads are kept alive and ready.  Many payment tasks are submitted
    at once and run concurrently, cutting wall-clock time dramatically.

Key concept:
    ThreadPoolExecutor manages thread lifecycle so we never spin up
    hundreds of threads and overload the system.  max_workers caps
    the concurrency level to something sensible.
"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import List

from models.order import Order


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAX_WORKERS       = 5     # threads kept alive in the pool
PAYMENT_FAIL_RATE = 0.1   # 10 % of payments fail (simulates real-world noise)


# ---------------------------------------------------------------------------
# Core payment logic (runs inside a pool thread)
# ---------------------------------------------------------------------------

def _charge_customer(order: Order) -> Order:
    """
    Simulate charging the customer for *order*.

    Runs inside a pool worker thread – must be thread-safe.
    (It only reads/writes the local *order* object, so it is safe.)

    Returns
    -------
    Order
        The same order with status='paid'.

    Raises
    ------
    RuntimeError
        When the simulated payment gateway rejects the charge.
    """
    # Simulate network latency to a payment gateway
    latency = random.uniform(0.15, 0.45)
    time.sleep(latency)

    if random.random() < PAYMENT_FAIL_RATE:
        raise RuntimeError(f"Payment gateway rejected order {order.id}")

    order.status = "paid"
    print(
        f"   [$$] [PaymentPool] [{order.id}] Charged  "
        f"${order.price:,.2f}  in {latency:.3f}s"
    )
    return order


# ---------------------------------------------------------------------------
# Public worker-pool API
# ---------------------------------------------------------------------------

class PaymentPool:
    """
    Manages a ThreadPoolExecutor for parallel payment processing.

    Usage (single order)
    --------------------
    pool = PaymentPool()
    paid_order = pool.process(order)

    Usage (batch)
    -------------
    pool = PaymentPool()
    results = pool.process_batch(orders)
    """

    def __init__(self, max_workers: int = MAX_WORKERS):
        self._max_workers = max_workers

    # ------------------------------------------------------------------
    # Single-order (blocking)
    # ------------------------------------------------------------------

    def process(self, order: Order) -> Order:
        """
        Submit *order* to the pool and block until payment completes.

        Returns the paid order or re-raises any payment error.
        """
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            future: Future = pool.submit(_charge_customer, order)
            return future.result()   # blocks; propagates RuntimeError on failure

    # ------------------------------------------------------------------
    # Batch (concurrent, non-blocking submission)
    # ------------------------------------------------------------------

    def process_batch(self, orders: List[Order]) -> List[Order]:
        """
        Submit all orders at once; process up to *max_workers* in parallel.

        Parameters
        ----------
        orders:
            List of Order objects with status='validated'.

        Returns
        -------
        list[Order]
            Successfully paid orders (failed ones are logged and skipped).
        """
        paid_orders: List[Order] = []

        print(
            f"\n   [**] [PaymentPool] Processing {len(orders)} orders "
            f"with {self._max_workers} workers..."
        )

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            # Submit all at once — the pool queues extras automatically
            future_to_order = {
                pool.submit(_charge_customer, order): order
                for order in orders
            }

            # Collect results as each future completes (not in submit order)
            for future in as_completed(future_to_order):
                original_order = future_to_order[future]
                try:
                    paid_order = future.result()
                    paid_orders.append(paid_order)
                except RuntimeError as exc:
                    print(f"   [!!] [PaymentPool] [{original_order.id}] FAILED - {exc}")

        print(
            f"   [OK] [PaymentPool] Batch complete: "
            f"{len(paid_orders)}/{len(orders)} succeeded\n"
        )
        return paid_orders



# ---------------------------------------------------------------------------
# Module-level singleton used by pipeline/stages.py
# ---------------------------------------------------------------------------

payment_pool = PaymentPool(max_workers=MAX_WORKERS)
