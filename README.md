# python-multiprocessing-queue-comparison
See (results.txt)[https://github.com/marcotollini/python-multiprocessing-queue-comparison/blob/main/results.txt] or run the tests via `python3 test.py`.

The results are created using one producer and `pool_size` consumers. The consumer (worker) does nothing with the job received: it just fetch the string and discard it
