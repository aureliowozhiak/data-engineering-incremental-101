import json
from datetime import datetime
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import Any, Callable


def ensure_directory(path: Path) -> None:
    """Garante que o diretório existe."""
    path.mkdir(parents=True, exist_ok=True)


def log(message: str) -> None:
    """Imprime logs com timestamp para facilitar leitura."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def timed_step(step_name: str) -> Callable:
    """Decorator para medir tempo de execução de um método."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            start = perf_counter()
            result = func(*args, **kwargs)
            elapsed = perf_counter() - start
            log(f"{step_name}: {elapsed:.2f} segundos")
            return result, elapsed

        return wrapper

    return decorator


def read_metadata(metadata_path: Path) -> dict:
    """Lê metadata do controle incremental."""
    if not metadata_path.exists():
        default_data = {"last_processed_id": 0}
        write_metadata(metadata_path, default_data)
        return default_data

    with metadata_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_metadata(metadata_path: Path, metadata: dict) -> None:
    """Atualiza metadata no arquivo json."""
    ensure_directory(metadata_path.parent)
    with metadata_path.open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)
