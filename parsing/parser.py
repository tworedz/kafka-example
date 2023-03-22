import abc
from typing import AsyncIterator


class BaseParser(abc.ABC):
    topic: str

    @classmethod
    @abc.abstractmethod
    async def parse(cls, search: str) -> AsyncIterator:
        raise NotImplementedError()
