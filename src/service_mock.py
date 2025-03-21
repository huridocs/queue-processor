from src.queue_processor.QueueProcessor import QueueProcessor


def process(message: dict[str, any]) -> tuple[dict[str, any] | None, bool]:
    if "required_field" not in message:
        return None, True

    message["processed"] = True
    return message, True


if __name__ == "__main__":
    queue_processor = QueueProcessor("localhost", 6380, ["test_queue_1", "test_queue_2"])
    queue_processor.start(process)
