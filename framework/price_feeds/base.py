import time
from abc import ABC, abstractmethod


class PriceFeed(ABC):
    """Abstract base class for venue price feeds."""

    def __init__(self):
        self._last_success: float = 0.0

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        ...

    @abstractmethod
    async def get_price(self, symbol: str) -> float | None:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    def is_healthy(self) -> bool:
        return (time.time() - self._last_success) < 60.0
