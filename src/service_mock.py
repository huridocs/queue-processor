from src.QueueProcessor import QueueProcessor


def process(message: dict[str, any]) -> dict[str, any]:
    message["processed"] = True
    return message


if __name__ == "__main__":
    queue_processor = QueueProcessor("localhost", 6380, ["test_queue_1", "test_queue_2"])
    queue_processor.run(process)
