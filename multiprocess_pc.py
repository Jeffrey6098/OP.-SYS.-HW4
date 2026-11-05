import multiprocessing
import time
import random


BUFFER_CAPACITY = 5
NUM_PRODUCERS = 3
NUM_CONSUMERS = 2
ITEMS_PER_PRODUCER = 3

def producer(producer_id, shared_buffer, print_lock, buf_count, total_produced):
    prod_num = producer_id + 1
    for i in range(ITEMS_PER_PRODUCER):
        time.sleep(random.random()* 0.4)
        item = f"item {i} from Producer {prod_num}"

        shared_buffer.put(item)

        with print_lock:
            buf_count.value += 1  # Increment buffer count
            total_produced.value += 1  # Increment total produced count
            print(f"[Producer {prod_num}] produced item {i}. Buffer size: {shared_buffer.qsize()}")

   
def consumer(consumer_id, shared_buffer, print_lock, buf_count, total_consumed):
    con_num = consumer_id + 1
    while True:

        item = shared_buffer.get()
        with print_lock:
            if item is None:
                print(f"[Consumer {con_num}] received shutdown signal. Exiting.")
                break

            buf_count.value -= 1  # Decrement buffer count
            total_consumed.value += 1  # Increment total consumed count
            print(f"[Consumer {con_num}] consumed {item}. Buffer size: {shared_buffer.qsize()}")

        time.sleep(random.random()*.9)  # Random sleep to simulate 



def run():

    start_time = time.perf_counter()
    shared_buffer = multiprocessing.Queue(maxsize=BUFFER_CAPACITY)  # Shared buffer for producer-consumer
    print_lock = multiprocessing.Lock()
    buf_count = multiprocessing.Value('i', 0)  # Shared counter for buffer size
    total_produced = multiprocessing.Value('i', 0)  # Shared counter for total produced items
    total_consumed = multiprocessing.Value('i', 0)  # Shared counter for total consumed items

    producers =[multiprocessing.Process(target=producer, args=(i, shared_buffer, print_lock, buf_count, total_produced)) for i in range(NUM_PRODUCERS)]
    consumers =[multiprocessing.Process(target=consumer, args=(i, shared_buffer, print_lock, buf_count, total_consumed)) for i in range(NUM_CONSUMERS)]

    for p in producers:
        p.start()
    for c in consumers:
        c.start()

    for p in producers:
        p.join()

    for _ in range(NUM_CONSUMERS):
        shared_buffer.put(None)  # Send poison pill to each consumer

    for c in consumers:
        c.join()

    elapsed_time = time.perf_counter() - start_time
    print(
        f"\nall producers finished ({total_produced.value} produced), "
        f"all consumers finished ({total_consumed.value} consumed), "
        f"{buf_count.value} items left"
    )
    print(f"Total execution time (multiprocessing.Queue): {elapsed_time:.4f} seconds")



if __name__ == "__main__":
    run()