import multiprocessing
import time
import random

def producer(queue, items_to_produce):
    for i in range(items_to_produce):
        item = f"Item-{i}"
        print(f"Producing {item}")
        queue.put(item)
        time.sleep(random.uniform(0.1, 0.5))  # Simulate variable production time

    # Signal the consumers that no more items will be produced
    for _ in range(num_consumers):
        queue.put(None)

def consumer(queue, consumer_id):
    while True:
        item = queue.get()
        if item is None:
            break  # Exit the loop when a sentinel value is received
        print(f"Consumer-{consumer_id} consuming {item}")
        time.sleep(random.uniform(0.1, 0.5))  # Simulate variable consumption time

if __name__ == "__main__":
    items_to_produce = 100
    num_consumers = 3

    # Create a multiprocessing queue to share data between processes
    shared_queue = multiprocessing.Queue()

    # Create and start the producer process
    producer_process = multiprocessing.Process(target=producer, args=(shared_queue, items_to_produce))
    producer_process.start()

    # Create and start the consumer processes
    consumer_processes = []
    for i in range(num_consumers):
        consumer_process = multiprocessing.Process(target=consumer, args=(shared_queue, i))
        consumer_processes.append(consumer_process)
        consumer_process.start()

    # Wait for the producer to finish
    producer_process.join()

    # Wait for the consumers to finish
    for consumer_process in consumer_processes:
        consumer_process.join()

    print("All processes finished.")
