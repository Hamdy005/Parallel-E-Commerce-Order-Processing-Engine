from dataclasses import dataclass

@dataclass
class Order:
    id: str
    item: str
    quantity: int
    price: float
    status: str = "pending"
