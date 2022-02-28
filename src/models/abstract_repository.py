from __future__ import annotations

import abc
from typing import Any

from pydantic import NonNegativeInt


class AbstractRepository(abc.ABC):
    """Abstract repository class"""

    def __init__(self, items: list[Any] | tuple[Any] | set[Any]) -> None:
        self._items = set(items)

    def __add__(self, other: AbstractRepository) -> AbstractRepository:
        return self.__class__(self._items | other._items)

    def __iter__(self):
        yield from iter(self._items)

    def __len__(self) -> NonNegativeInt:
        return NonNegativeInt(len(self._items))

    def add(self, item: Any) -> None:
        self._items.add(item)

    def find(self, **kwargs) -> AbstractRepository:
        filtered_items = [item for attr, value in kwargs.items() for item in self._items if
                             hasattr(item, attr) and getattr(item, attr) == value]
        return self.__class__(filtered_items)

    def get_all(self) -> list[Any]:
        return list(self._items)

    def get_one(self) -> Any:
        for item in self._items:
            return item

    def by_id(self, id: str) -> Any:
        return self.find(id=id).get_one()
