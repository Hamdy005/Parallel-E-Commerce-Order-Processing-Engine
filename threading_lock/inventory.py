"""
threading_lock/inventory.py  –  Dev B: Concurrency Engine
==========================================================
Thread-safe inventory management.

Problem solved:
    When multiple worker threads try to deduct stock at the same time,
    they might all read the same quantity, think there is enough stock,
    and all succeed — leaving inventory in a negative / corrupted state.
    This is called a *race condition*.

Solution:
    A threading.Lock forces threads to take turns.  Only one thread may
    read AND write the stock level at a time, so every deduction is safe.
"""

import os
import threading


class InventoryManager:
    """
    Shared inventory store with lock-protected stock operations.

    Attributes
    ----------
    _stock : dict[str, int]
        Maps item name → available quantity.
    _lock : threading.Lock
        Mutual-exclusion lock; one thread at a time modifies _stock.
    """

    def __init__(self, initial_stock: dict[str, int] | None = None):
        """
        Parameters
        ----------
        initial_stock:
            Optional seed stock.  Defaults to an empty catalogue.
        """
        self._initial_stock: dict[str, int] = initial_stock.copy() if initial_stock else {}
        self._stock: dict[str, int] = self._initial_stock.copy()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_item(self, item: str, quantity: int) -> None:
        """Add *quantity* units of *item* to the catalogue (thread-safe)."""
        if quantity <= 0:
            raise ValueError(f"Quantity to add must be positive, got {quantity}")

        with self._lock:                          # 🔐 acquire lock
            self._stock[item] = self._stock.get(item, 0) + quantity
            if os.environ.get("SHOW_LOCK_TRACKING", "1") == "1":
                print(
                    f"   [+] [Inventory] Added {quantity}x '{item}'  "
                    f"-> stock now {self._stock[item]}"
                )
                                                  # 🔓 lock released automatically

    def deduct_stock(self, order) -> None:
        """
        Deduct *order.quantity* units of *order.item* from stock.

        Thread-safe: the lock ensures no two threads can read and write
        the same stock level simultaneously (no race condition).

        Raises
        ------
        RuntimeError
            If the item is not in the catalogue or stock is insufficient.
        """
        with self._lock:                          # 🔐 acquire lock
            item     = order.item
            needed   = order.quantity
            in_stock = self._stock.get(item, 0)

            if in_stock < needed:
                raise RuntimeError(
                    f"[{order.id}] Insufficient stock for '{item}': "
                    f"need {needed}, have {in_stock}"
                )

            self._stock[item] = in_stock - needed
            if os.environ.get("SHOW_LOCK_TRACKING", "1") == "1":
                print(
                    f"   [L] [Inventory] [{order.id}] Deducted {needed}x '{item}'  "
                    f"-> stock now {self._stock[item]}"
                )
                                                  # 🔓 lock released automatically

    def get_stock(self, item: str) -> int:
        """Return current stock level for *item* (thread-safe, read-only)."""
        with self._lock:
            return self._stock.get(item, 0)

    def snapshot(self) -> dict[str, int]:
        """Return a point-in-time copy of the full stock catalogue."""
        with self._lock:
            return self._stock.copy()

    def reset(self) -> None:
        """Restore stock to its original levels (call before each run)."""
        with self._lock:
            self._stock = self._initial_stock.copy()

    def __repr__(self) -> str:
        return f"InventoryManager(items={len(self._stock)}, stock={dict(self._stock)})"


# Seeded with every item from producer_consumer/items.txt (50 units each).
# This ensures orders always find a matching catalogue entry.
import os as _os
_ITEMS_FILE = _os.path.join(
    _os.path.dirname(_os.path.abspath(__file__)),
    "..", "producer_consumer", "items.txt"
)
with open(_ITEMS_FILE, "r") as _f:
    _items_from_file = [line.strip() for line in _f if line.strip()]

inventory = InventoryManager(
    initial_stock={item: 50 for item in _items_from_file}
)
