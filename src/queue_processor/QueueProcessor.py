import logging
from rsmq.cmd import NoMessageInQueue, utils
from rsmq import RedisSMQ, cmd
import time

handlers = [logging.StreamHandler()]
logging.root.handlers = []
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)
queue_processor_logger = logging.getLogger(__name__)


class QueueProcessor:
    def __init__(self, redis_host: str, redis_port: int, queues_names_by_priority: list[str]):
        self.redis_host: str = redis_host
        self.redis_port: int = redis_port
        self.task_queues_names: list[str] = [queue_name + "_tasks" for queue_name in queues_names_by_priority]
        self.results_queues_names: list[str] = [queue_name + "_results" for queue_name in queues_names_by_priority]


    def get_queue(self, queue_name):
        return RedisSMQ(host=self.redis_host, port=self.redis_port, qname=queue_name)

    def create_queue(self, queue_name: str):
        try:
            self.get_queue(queue_name).getQueueAttributes().exec_command()
        except cmd.exceptions.QueueDoesNotExist:
            queue_processor_logger.info(f"Creating queue")
            self.get_queue(queue_name).createQueue().vt(120).exceptions(False).execute()

    def start(self, process: callable) -> bool:
        queue_processor_logger.info("Running")
        while True:
            try:
                for queue_name in self.task_queues_names + self.results_queues_names:
                    self.create_queue(queue_name)

                for task_queue_name, results_queue_name in zip(self.task_queues_names, self.results_queues_names):
                    task_queue = self.get_queue(task_queue_name)
                    message = task_queue.receiveMessage().execute()

                    if "message" in message:
                        message["message"] = utils.decode_message(message["message"])

                    task_queue.deleteMessage(qname=task_queue_name, id=message["id"]).execute()

                    results = process(message["message"])
                    self.get_queue(results_queue_name).sendMessage().message(results).execute()

            except NoMessageInQueue:
                time.sleep(2)
            except Exception as e:
                queue_processor_logger.error(f"Error: {e}", exc_info=True)
                return False
