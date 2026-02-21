from abc import ABC, abstractmethod


class BaseConnector(ABC):
    @abstractmethod
    def fetch(self, source_url: str) -> str:
        raise NotImplementedError
