from time import sleep

from rsmq import RedisSMQ

QUEUES_NAMES = ["development_ocr"]

if __name__ == "__main__":
    for i in range(100):
        for queue_name in QUEUES_NAMES:
            try:
                print(queue_name)
                queue = RedisSMQ(host="localhost", qname=f"{queue_name}_tasks")
                print(queue.getQueueAttributes().exec_command())
            except:
                pass
            print()

        sleep(5)
