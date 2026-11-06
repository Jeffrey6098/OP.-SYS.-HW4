# OP.-SYS.-HW4
Changes:
All threads share the same memory space so bound buffer is just a python
list in the code. However for multiprocessing, memory space is not shared, so 
I use multiprocessing.Queue(maxsize=BUFFER_CAPACITY) to pickle items and sends 
them across OS pipe.

Unlike threading, which uses cond.wait(), with cond:, cond.notify_all(), 
mulriprocessing.Queue() automatically handles the buffer locking and blocking
with .put() and .get(). However, processes run in parelle so they can also print
to the consol in parallel which is why the print_lock is added so the output
is not jumbled.

GIL cost/Performance comparison
I/O-Bound workload favors threading because multiprocessing has a higher
overhead from the need to pickle items through a pipe. It also has a higher
startup cost to go along with it 

CPU-Bound workload favors multiprocessing due to its ability to bypass
the GIL since each process is given its own GIL creatiing parallelism whereass
threading is bound by the one GIL in the process.
