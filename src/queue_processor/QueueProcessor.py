from typing import Any
import logging
from time import sleep

import redis
from rsmq.cmd import NoMessageInQueue, utils
from rsmq import RedisSMQ, cmd

from queue_processor.QueueProcess import QueueProcess
from queue_processor.QueueProcessResults import QueueProcessResults


class QueueProcessor:
    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        queues_names_by_priority: list[str],
        logger: logging.Logger = None,
        delay_time_for_results: int = 0,
    ):

        self.redis_host: str = redis_host
        self.redis_port: int = redis_port
        self.task_queues_names: list[str] = [queue_name + "_tasks" for queue_name in queues_names_by_priority]
        self.results_queues_names: list[str] = [queue_name + "_results" for queue_name in queues_names_by_priority]
        self.delay_time_for_results = delay_time_for_results
        self.exists_queues = False
        if logger:
            self.queue_processor_logger = logger
            return

        handlers = [logging.StreamHandler()]
        logging.root.handlers = []
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)
        self.queue_processor_logger = logging.getLogger(__name__)

    def send_message(self, message: dict[str, Any]):
        self.get_queue(self.task_queues_names[0]).sendMessage(delay=0).message(message).execute()

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
                self.get_queue(queue_name).createQueue(maxsize=-1).vt(120).exceptions(False).execute()

    def start(self, queue_process: QueueProcess):
        self.queue_processor_logger.info("QueueProcessor running")
        while True:
            for task_queue_name, results_queue_name in zip(self.task_queues_names, self.results_queues_names):
                try:
                    self.create_queues()
                    task_queue = self.get_queue(task_queue_name)
                    raw_message = task_queue.receiveMessage().execute()
                except NoMessageInQueue:
                    task_queue = self.get_queue(task_queue_name)
                    raw_message = None
                except redis.exceptions.ConnectionError:
                    self.exists_queues = False
                    self.queue_processor_logger.error(f"Error connecting to Redis: {self.redis_host}:{self.redis_port}")
                    sleep(30)
                    break
                except Exception as e:
                    self.exists_queues = False
                    self.queue_processor_logger.error(f"Error: {e}", exc_info=True)
                    sleep(30)
                    break

                try:
                    if raw_message:
                        message = utils.decode_message(raw_message["message"])
                        queue_processor_results: QueueProcessResults = queue_process.process_message(message)

                        if queue_processor_results.delete_message:
                            task_queue.deleteMessage(qname=task_queue_name, id=raw_message["id"]).execute()
                        else:
                            task_queue.changeMessageVisibility(
                                id=raw_message["id"], vt=queue_processor_results.invisibility_timeout
                            ).execute()
                    else:
                        queue_processor_results: QueueProcessResults = queue_process.process(task_queue_name)

                    if not queue_processor_results.results:
                        continue

                    self.get_queue(results_queue_name).sendMessage(delay=self.delay_time_for_results).message(
                        queue_processor_results.results
                    ).execute()
                    sleep(0.1)
                except Exception as e:
                    self.queue_processor_logger.error(f"Error: {e}", exc_info=True)
                    sleep(30)
