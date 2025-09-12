from typing import Any
from src.queue_processor.QueueProcessResults import QueueProcessResults
from src.queue_processor.QueueProcessor import QueueProcessor
from src.queue_processor.QueueProcess import QueueProcess


class MockQueueProcess(QueueProcess):
    def process_message(self, queue_name: str, message: dict[str, Any]) -> QueueProcessResults:
        if "required_field" not in message:
            return QueueProcessResults()

        message["processed"] = True
        return QueueProcessResults(results=message)

    def process(self, queue_name: str) -> QueueProcessResults:
        return QueueProcessResults()


if __name__ == "__main__":
    queue_processor = QueueProcessor("localhost", 6380, ["test_queue_1", "test_queue_2"])
    mock_process = MockQueueProcess()
    queue_processor.start(mock_process)
