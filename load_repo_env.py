"""Ładuje zmienne z `.env` w katalogu głównym repozytorium."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent


def bootstrap() -> Path:
    """Wczytuje `REPO_ROOT/.env` do `os.environ` i zwraca ścieżkę roota repo.

    Returns:
        Katalog główny repozytorium (tam gdzie leży `.env`).
    """
    load_dotenv(REPO_ROOT / ".env")
    return REPO_ROOT


def require_env(name: str) -> str:
    """Zwraca wartość zmiennej lub rzuca wyjątek, jeśli brak / pusta.

    Args:
        name: Nazwa zmiennej środowiskowej.

    Returns:
        Niepusty string.

    Raises:
        RuntimeError: Gdy zmienna nie istnieje lub jest pusta.
    """
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        msg = (
            f"Brak zmiennej środowiskowej {name}. "
            "Uzupełnij plik .env w katalogu głównym projektu (wzorz: .env.example)."
        )
        raise RuntimeError(msg)
    return value
