## How to use it

1. Install the repository

```commandline
pip install git+https://github.com/huridocs/queue-processor
```


2. Use it like this:

```python
from queue_processor import QueueProcessor

def process_function(task_message):
    result_message = task_message
    result_message['processed'] = True
    return result_message

queue_processor = QueueProcessor(redis_host="localhost", redis_port=6379, queues_names_by_priority=["test_queue_1", "test_queue_2"])
queue_processor.start()

print("the process stopped unexpectedly")
```