import threading
import time
import random


BUFFER_CAPACITY = 1
NUM_PRODUCERS = 3
NUM_CONSUMERS = 2
ITEMS_PER_PRODUCER = 3
buffer = []
cond = threading.Condition()  # Condition variable for synchronizing producers and consumers

total_produced = 0
total_consumed = 0

def producer(producer_id):
    global total_produced
    p_id=producer_id+1
    # Hint 1: Loop through the number of items this producer needs to generate.
    for i in range(ITEMS_PER_PRODUCER):
        #random sleep shakes out race conditions and simulates I/O-bound work
        time.sleep(random.random() * .4)    
        #create item
        item = f"item {i} from Producer {p_id}" 
   
        with cond:
            waited = False
            while len(buffer)== BUFFER_CAPACITY:
                # only prin waiting message the first time we wait, not every time we wake up and check the condition
                if not waited:
                    print(f"[Producer {p_id}] buffer full - waiting.")
                    waited = True
                cond.wait()

            if waited:
                print(f"[Producer {p_id}] resumed.")

            buffer.append(item)  
            total_produced += 1
            print(f"[Producer {p_id}] produced item {i}. Buffer size: {len(buffer)}")
            cond.notify_all()

def consumer(consumer_id):
    global total_consumed
    c_id = consumer_id + 1
    
    while True:
        with cond:
            while len(buffer) == 0:
                cond.wait()

            item = buffer.pop(0)

            #if None it is poison pill, signal to exit
            if item is None:
                print(f"[Consumer {c_id}] received shutdown signal. Exiting.")
                cond.notify_all()  
                break 

            #read from buffer and process item
            print(f"[Consumer {c_id}] consumed {item}. Buffer size: {len(buffer)}")
            total_consumed += 1
            cond.notify_all()    

        time.sleep(random.random() * .9)  # Random sleep to simulate processing time and shake out race conditions




def run():
   
    start_time = time.perf_counter()
    #create consumer and producer threads
    consumers = [threading.Thread(target=consumer, args=(i,)) for i in range(NUM_CONSUMERS)]
    producers = [threading.Thread(target=producer, args=(i,)) for i in range(NUM_PRODUCERS)]
    
    # Start all producer and consumer threads
    for p in producers:
        p.start()
   
    for c in consumers:
        c.start()

    # Wait for all producer threads to finish
    for p in producers:
        p.join()

    #send poison pills to consumers to signal them to exit
    for _ in range(NUM_CONSUMERS):
        with cond:
            while len(buffer) == BUFFER_CAPACITY:
                    cond.wait()
            buffer.append(None)  # Insert a poison pill for each consumer
            cond.notify_all()    # Notify consumers that a new item (poison pill) is available

    # Wait for all consumer threads to finish
    for c in consumers:
        c.join()
    
    end_time = time.perf_counter()
    print(
        f"\nall producers finished ({total_produced} produced), "
        f"all consumers finished ({total_consumed} consumed), "
        f"{len(buffer)} items left"
    )
    print(f"Total execution time (threading): {end_time - start_time:.4f} seconds")


if __name__ == "__main__":
    #created a run() functions to encapsulate the main logic and allow for easier testing and reuse of code
    for _ in range(3):
        print("\nStarting new run of producer-consumer simulation...\n")
        run()
        total_produced = 0
        total_consumed = 0
        #can edit global variables here to test different scenarios without restarting the program
        #BUFFER_CAPACITY+=1
        #NUM_PRODUCERS+=1
        #NUM_CONSUMERS+=1
