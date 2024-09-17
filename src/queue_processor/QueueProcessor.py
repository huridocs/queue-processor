import logging
from time import sleep

import redis
from rsmq.cmd import NoMessageInQueue, utils
from rsmq import RedisSMQ, cmd


class QueueProcessor:
    def __init__(self, redis_host: str, redis_port: int, queues_names_by_priority: list[str], logger: logging.Logger = None):
        self.redis_host: str = redis_host
        self.redis_port: int = redis_port
        self.task_queues_names: list[str] = [queue_name + "_tasks" for queue_name in queues_names_by_priority]
        self.results_queues_names: list[str] = [queue_name + "_results" for queue_name in queues_names_by_priority]

        self.exists_queues = False
        if logger:
            self.queue_processor_logger = logger
            return

        handlers = [logging.StreamHandler()]
        logging.root.handlers = []
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)
        self.queue_processor_logger = logging.getLogger(__name__)

    def get_queue(self, queue_name):
        return RedisSMQ(host=self.redis_host, port=self.redis_port, qname=queue_name)

    def create_queues(self):
        if self.exists_queues:
            return

        for queue_name in self.task_queues_names + self.results_queues_names:
            try:
                self.get_queue(queue_name).getQueueAttributes().exec_command()
            except cmd.exceptions.QueueDoesNotExist:
                self.queue_processor_logger.info(f"Creating queue {queue_name}")
                self.get_queue(queue_name).createQueue().vt(120).exceptions(False).execute()

    def start(self, process: callable):
        self.queue_processor_logger.info("QueueProcessor running")
        while True:
            for task_queue_name, results_queue_name in zip(self.task_queues_names, self.results_queues_names):
                try:
                    self.create_queues()
                    self.queue_processor_logger.info(f"Processing queue: {task_queue_name}")
                    task_queue = self.get_queue(task_queue_name)
                    message = task_queue.receiveMessage().execute()
                    task_queue.deleteMessage(qname=task_queue_name, id=message["id"]).execute()

                    if "message" in message:
                        message["message"] = utils.decode_message(message["message"])

                    results = process(message["message"])
                    self.queue_processor_logger.info(f"Sending results to queue: {results_queue_name}")
                    self.get_queue(results_queue_name).sendMessage().message(results).execute()
                    break
                except NoMessageInQueue:
                    self.queue_processor_logger.info("No messages in queue")
                    sleep(2)
                except redis.exceptions.ConnectionError:
                    self.exists_queues = False
                    self.queue_processor_logger.error(f"Error connecting to Redis: {self.redis_host}:{self.redis_port}")
                    sleep(30)
                except Exception as e:
                    self.exists_queues = False
                    self.queue_processor_logger.error(f"Error: {e}", exc_info=True)
                    sleep(60)
