from contextlib import contextmanager
from datetime import datetime


@contextmanager
def log_execution(task_name):
    """Print execution time of any block."""
    start = datetime.now()
    print(f"   ⏳ {task_name} started...")
    yield
    end = datetime.now()
    duration = (end - start).total_seconds()
    print(f"   ✅ {task_name} completed in {duration:.4f}s")