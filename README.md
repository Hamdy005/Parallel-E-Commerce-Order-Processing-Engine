# 📦 E-Commerce Order Processing System (Parallel Processing Project)

## 🚀 Overview

This project simulates a real-world **e-commerce order processing backend system** using parallel programming concepts in Python.

It demonstrates how modern systems handle high workloads using:

- 🔁 Producer–Consumer Pattern
- 📦 Queue-based Communication
- 🧩 Pipeline Architecture
- ⚡ Worker Pool (Concurrency)
- 🔐 Threading with Locks
- 🖥️ Multiprocessing (Scalability Layer)

---

## ⚙️ System Goal

Efficiently process customer orders through multiple stages:

```text
📥 Order Creation
      ↓
📦 Queue System
      ↓
🧩 Pipeline Processing
   ├── Validate Order
   ├── 💳 Payment Processing (Worker Pool)
   ├── 🔐 Inventory Update (Locks)
   └── 🚚 Shipping
      ↓
✅ Completed Orders
```

---

## 📊 Sample Execution Output

```text
=======================================================
  E-Commerce Order Processing System
  Pattern: Producer-Consumer + Pipeline
=======================================================

[ORD-001] COMPLETE – shipped
[ORD-002] FAILED – Payment failed
[ORD-003] COMPLETE – shipped
[ORD-004] COMPLETE – shipped

=======================================================
  Parallel Execution Time Report:
   Worker 1 → 1.10s
   Worker 2 → 0.95s
   Worker 3 → 1.05s
-------------------------------------------------------
  Total Time: 1.10s
  Throughput: 5.45 orders/sec
=======================================================
```

---

## ⏱️ Sequential Pipeline Execution Time Report

```text
=======================================================
  Sequential Execution Time Report:
   Stage: Validate Order  → 0.55s
   Stage: Payment         → 1.45s
   Stage: Inventory       → 0.70s
   Stage: Shipping        → 0.50s
-------------------------------------------------------
  Total Time: 3.20s
  Throughput: 1.25 orders/sec
=======================================================
```

---

## 📁 Project Structure

```text
Project/
│
├── main.py
├── README.md
│
├── models/
│   └── order.py
│
├── producer_consumer/
│   ├── producer_consumer.py
│   ├── queue_manager.py
│   └── items.txt
│
├── pipeline/
│   └── stages.py
│
├── threading_lock/
│   └── inventory.py
│
├── worker_pool/
│   └── payment_pool.py
│
├── multiprocessing_system/
│   └── process_manager.py
│
└── utils/
    └── logger.py
```

---

## 👨‍💻 Developer Responsibilities

### 🟦 Dev A — Flow & Pipeline (CORE SYSTEM)
**Role (Simple)**

Builds how orders move through the system.

**Responsibilities**
- Create order queue system 📦
- Implement producer–consumer flow 🔁
- Build processing pipeline 🧩

**Files**
- `producer_consumer/queue_manager.py`
- `pipeline/stages.py`

---

### 🟩 Dev B — Concurrency Engine ⚡
**Role (Simple)**

Makes system safe and fast when multiple orders run at the same time.

**Responsibilities**
- Protect shared inventory (no race conditions) 🔐
- Run payment processing in parallel 💳

**Files**
- `threading_lock/inventory.py`
- `worker_pool/payment_pool.py`

**Key Idea**
Locks prevent data corruption.
Worker pool improves performance.

---

### 🟨 Dev C — Scaling Layer 🖥️
**Role (Simple)**

Makes the system run across multiple processes.

**Responsibilities**
- Use multiprocessing for scaling 🚀
- Distribute workload across CPU cores

**Files**
- `multiprocessing_system/process_manager.py`

**Key Idea**
True parallel execution.
Better performance for large loads.
