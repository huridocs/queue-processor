from abc import ABC, abstractmethod

from queue_processor.QueueProcessResults import QueueProcessResults


class QueueProcess(ABC):

    @abstractmethod
    def process_message(self, message) -> QueueProcessResults:
        pass

    @abstractmethod
    def process(self, task_queue_name: str) -> QueueProcessResults:
        pass
