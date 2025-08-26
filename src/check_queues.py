from time import sleep

from rsmq import RedisSMQ

QUEUES_NAMES = ["production_information_extraction"]

if __name__ == "__main__":
    for i in range(100):
        for queue_name in QUEUES_NAMES:
            try:
                print(f"{queue_name}_tasks")
                queue = RedisSMQ(host="localhost", qname=f"{queue_name}_tasks")
                print(queue.getQueueAttributes().exec_command())
                print(f"{queue_name}_results")
                queue = RedisSMQ(host="localhost", qname=f"{queue_name}_results")
                print(queue.getQueueAttributes().exec_command())
            except:
                print("queue does not exist")
                pass
            print()

        sleep(5)
