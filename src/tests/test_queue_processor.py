from time import sleep
from unittest import TestCase
from rsmq import RedisSMQ
from rsmq.cmd import utils
from rsmq.cmd.exceptions import NoMessageInQueue


class TestQueueProcessor(TestCase):

    def empty_queue(self, queue):
        """Empty a queue by receiving and deleting all messages"""
        while True:
            try:
                message = queue.receiveMessage().execute()
                queue.deleteMessage(id=message["id"]).execute()
            except NoMessageInQueue:
                break

    def test_two_queues(self):
        queue_tasks_1 = RedisSMQ(host="localhost", port=6380, qname="test_queue_1_tasks")
        queue_tasks_2 = RedisSMQ(host="localhost", port=6380, qname="test_queue_2_tasks")
        queue_results_1 = RedisSMQ(host="localhost", port=6380, qname="test_queue_1_results")
        queue_results_2 = RedisSMQ(host="localhost", port=6380, qname="test_queue_2_results")

        # Create queues if they don't exist
        for queue in [queue_tasks_1, queue_tasks_2, queue_results_1, queue_results_2]:
            try:
                queue.getQueueAttributes().execute()
            except:
                queue.createQueue(maxsize=-1).vt(120).exceptions(False).execute()

        # Empty all queues
        self.empty_queue(queue_tasks_1)
        self.empty_queue(queue_tasks_2)
        self.empty_queue(queue_results_1)
        self.empty_queue(queue_results_2)

        sleep(1)

        queue_tasks_1.sendMessage().message({"test": "test_0"}).execute()
        queue_tasks_1.sendMessage().message({"required_field": True, "test": "test_1"}).execute()
        queue_tasks_1.sendMessage().message({"required_field": True, "test": "test_2"}).execute()
        queue_tasks_2.sendMessage().message({"test": "test_0"}).execute()
        queue_tasks_2.sendMessage().message({"required_field": True, "test": "test_3"}).execute()

        sleep(5)

        result_message_1 = utils.decode_message(queue_results_1.receiveMessage().execute()["message"])
        result_message_2 = utils.decode_message(queue_results_1.receiveMessage().execute()["message"])
        result_message_3 = utils.decode_message(queue_results_2.receiveMessage().execute()["message"])

        self.assertEqual(result_message_1["test"], "test_1")
        self.assertTrue(result_message_1["processed"])
        self.assertEqual(result_message_2["test"], "test_2")
        self.assertTrue(result_message_2["processed"])
        self.assertEqual(result_message_3["test"], "test_3")
        self.assertTrue(result_message_3["processed"])
