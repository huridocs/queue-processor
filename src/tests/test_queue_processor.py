from time import sleep
from unittest import TestCase
from rsmq import RedisSMQ
from rsmq.cmd import utils


class TestQueueProcessor(TestCase):

    def test_two_queues(self):
        queue_tasks_1 = RedisSMQ(host="localhost", port=6380, qname="test_queue_1_tasks")
        queue_tasks_2 = RedisSMQ(host="localhost", port=6380, qname="test_queue_2_tasks")
        queue_results_1 = RedisSMQ(host="localhost", port=6380, qname="test_queue_1_results")
        queue_results_2 = RedisSMQ(host="localhost", port=6380, qname="test_queue_2_results")

        queue_tasks_1.deleteQueue().exceptions(False).execute()
        queue_tasks_2.deleteQueue().exceptions(False).execute()
        queue_results_1.deleteQueue().exceptions(False).execute()
        queue_results_2.deleteQueue().exceptions(False).execute()

        sleep(3)

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
