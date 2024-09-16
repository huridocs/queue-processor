import logging
from rsmq.cmd import NoMessageInQueue, utils
from rsmq import RedisSMQ, cmd
import time

handlers = [logging.StreamHandler()]
logging.root.handlers = []
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)
queue_processor_logger = logging.getLogger(__name__)


class QueueProcessor:
    def __init__(self, redis_host: str, redis_port: int, queues_names: list[str]):
        self.task_queues_names: list[str] = list()
        self.task_queues: list[RedisSMQ] = list()
        self.results_queues: list[RedisSMQ] = list()
        for queue_name in queues_names:
            task_queue_name = f"{queue_name}_tasks"
            self.task_queues_names.append(task_queue_name)
            self.task_queues.append(
                RedisSMQ(
                    host=redis_host,
                    port=redis_port,
                    qname=task_queue_name,
                )
            )

            self.results_queues.append(
                RedisSMQ(
                    host=redis_host,
                    port=redis_port,
                    qname=f"{queue_name}_results",
                )
            )

    def create_queue(self, queue: RedisSMQ):
        try:
            queue.getQueueAttributes().exec_command()
        except cmd.exceptions.QueueDoesNotExist:
            queue_processor_logger.info(f"Creating queue")
            queue.createQueue().vt(120).exceptions(False).execute()

    def run(self, process: callable) -> bool:
        queue_processor_logger.info("Running")
        while True:
            try:
                for queue in self.task_queues + self.results_queues:
                    self.create_queue(queue)

                for task_queue_name, task_queue, results_queue in zip(
                    self.task_queues_names, self.task_queues, self.results_queues
                ):
                    message = task_queue.receiveMessage().execute()

                    if "message" in message:
                        message["message"] = utils.decode_message(message["message"])

                    task_queue.deleteMessage(qname=task_queue_name, id=message["id"]).execute()

                    results = process(message["message"])
                    results_queue.sendMessage().message(results).execute()

            except NoMessageInQueue:
                time.sleep(2)
            except Exception as e:
                queue_processor_logger.error(f"Error: {e}", exc_info=True)
                return False
