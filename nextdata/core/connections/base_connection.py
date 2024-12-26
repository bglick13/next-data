from abc import ABC, abstractmethod


class BaseConnection(ABC):
    def __init__(
        self,
    ):
        pass

    @abstractmethod
    def connect(self):
        pass
