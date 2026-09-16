import logging
import socket
import subprocess
import threading
import time
from time import sleep
from unittest import TestCase
import redis.exceptions
from rsmq import RedisSMQ
from rsmq.cmd import utils
from rsmq.cmd.exceptions import NoMessageInQueue

from queue_processor.QueueProcessResults import QueueProcessResults
from queue_processor.QueueProcessor import QueueProcessor


def redis_available(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def find_redis_container(port: int) -> str | None:
    """Find the docker container id serving redis on `port`, if any."""
    try:
        containers = subprocess.run(
            ["docker", "ps", "--format", "{{.ID}} {{.Image}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for line in containers.stdout.splitlines():
        container_id, image = line.split(maxsplit=1)
        if not image.startswith("redis"):
            continue
        inspect = subprocess.run(
            ["docker", "inspect", container_id, "--format", "{{.HostConfig.NetworkMode}} {{.Config.Cmd}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if f"--port {port}" in inspect.stdout:
            return container_id
    return None


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

        deadline = time.monotonic() + 60
        results = []
        while len(results) < 3 and time.monotonic() < deadline:
            for queue in (queue_results_1, queue_results_2):
                try:
                    results.append(utils.decode_message(queue.receiveMessage().execute()["message"]))
                except NoMessageInQueue:
                    pass
            sleep(0.5)

        self.assertEqual(len(results), 3, f"expected 3 results, got {results}")
        result_message_1, result_message_2, result_message_3 = sorted(results, key=lambda r: r["test"])
        self.assertNotEqual(result_message_1["test"], "test_0")
        self.assertNotEqual(result_message_2["test"], "test_0")

        self.assertEqual(result_message_1["test"], "test_1")
        self.assertTrue(result_message_1["processed"])
        self.assertEqual(result_message_2["test"], "test_2")
        self.assertTrue(result_message_2["processed"])
        self.assertEqual(result_message_3["test"], "test_3")
        self.assertTrue(result_message_3["processed"])

    def test_default_redis_options_propagate_to_client(self):
        processor = QueueProcessor("localhost", 6380, ["options_test"])
        queue = processor.get_queue("options_test_tasks")
        client = queue.client
        connection_kwargs = client.connection_pool.connection_kwargs
        self.assertEqual(connection_kwargs["socket_timeout"], 5.0)
        self.assertEqual(connection_kwargs["socket_connect_timeout"], 5.0)
        self.assertTrue(connection_kwargs["socket_keepalive"])
        self.assertEqual(connection_kwargs["health_check_interval"], 30)
        self.assertEqual(client.get_retry()._retries, 0)

    def test_custom_redis_options_override_defaults(self):
        processor = QueueProcessor(
            "localhost",
            6380,
            ["options_test"],
            redis_options={"socket_timeout": 1.5},
        )
        connection_kwargs = processor.get_queue("options_test_tasks").client.connection_pool.connection_kwargs
        self.assertEqual(connection_kwargs["socket_timeout"], 1.5)

    def test_receive_times_out_when_redis_paused(self):
        container = find_redis_container(6380)
        if container is None or not redis_available("localhost", 6380):
            self.skipTest("requires redis on port 6380 (docker compose)")

        processor = QueueProcessor("localhost", 6380, ["timeout_test"])
        processor.create_queues()
        queue = processor.get_queue("timeout_test_tasks")

        subprocess.run(["docker", "pause", container], check=True, timeout=10)
        try:
            start = time.monotonic()
            with self.assertRaises(redis.exceptions.TimeoutError):
                queue.receiveMessage().execute()
            elapsed = time.monotonic() - start
        finally:
            subprocess.run(["docker", "unpause", container], check=True, timeout=10)

        self.assertLess(elapsed, 15, "receiveMessage should time out within ~socket_timeout, not hang forever")

    def test_start_logs_and_recovers_when_redis_paused(self):
        container = find_redis_container(6380)
        if container is None or not redis_available("localhost", 6380):
            self.skipTest("requires redis on port 6380 (docker compose)")

        class FakeProcess:
            def process_message(self, queue_name, message):
                return QueueProcessResults()

            def process(self, queue_name):
                return QueueProcessResults()

        records: list[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("test_start_timeout")
        logger.addHandler(Capture())
        logger.setLevel(logging.ERROR)

        processor = QueueProcessor("localhost", 6380, ["start_timeout_test"], logger=logger)
        processor.create_queues()

        subprocess.run(["docker", "pause", container], check=True, timeout=10)
        thread = threading.Thread(target=processor.start, args=(FakeProcess(),), daemon=True)
        try:
            thread.start()
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if any("Redis" in rec.getMessage() for rec in records):
                    break
                sleep(0.1)
            self.assertTrue(
                any("Redis" in rec.getMessage() and rec.levelno >= logging.ERROR for rec in records),
                "start() should log a connection/timeout error within ~socket_timeout seconds",
            )
            self.assertFalse(processor.exists_queues)
        finally:
            subprocess.run(["docker", "unpause", container], check=True, timeout=10)
