from src.queue_processor.QueueProcessResults import QueueProcessResults
from src.queue_processor.QueueProcessor import QueueProcessor


def process(message: dict[str, any]) -> QueueProcessResults:
    if "required_field" not in message:
        return QueueProcessResults()

    message["processed"] = True
    return QueueProcessResults(results=message)


if __name__ == "__main__":
    queue_processor = QueueProcessor("localhost", 6380, ["test_queue_1", "test_queue_2"])
    queue_processor.start(process)
