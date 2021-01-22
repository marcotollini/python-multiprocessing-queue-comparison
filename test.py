import multiprocessing
import signal
import os
from time import time
import zmq

class WorkerSharedQueueProcess(multiprocessing.Process):
    def __init__(self, queue):
        super().__init__()
        self.__queue = queue

    def run(self):
        while True:
            submission = self.__queue.get(block=True)
            if submission is not None:
                # print(submission)
                continue
            else:
                return

class WorkerSharedQueueSwarm:
    def enqueue(self, job):
        self.__queue.put(job)

    def __init__(self, number_of_workers):
        self.__queue = multiprocessing.Queue()
        self.__processes = []
        for i in range(0, number_of_workers):
            t = WorkerSharedQueueProcess(self.__queue)
            self.__processes.append(t)

    def start(self):
        for process in self.__processes:
            process.start()

    def wait(self):
        for process in self.__processes:
            process.join()

    def stop(self):
        # self.__queue.empty()
        for process in self.__processes:
            self.__queue.put(None, block=True)



class PoolSharedQueue():
    def __init__(self, threads_number):
        self.ws = WorkerSharedQueueSwarm(threads_number)

        def term(sig, frame):
            self.ws.stop()
            self.ws.wait()

            if self.__prev_handler != 0:
                self.__prev_handler(sig, frame)

            os._exit(0)

        self.ws.start()

        self.__prev_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, term)

    def submit(self, job):
        self.ws.enqueue(job)

    def join(self):
        self.ws.stop()
        self.ws.wait()



#############################################################





class WorkerDedicatedQueueProcess(multiprocessing.Process):
    def __init__(self, queue):
        super().__init__()
        self.__queue = queue

    def run(self):
        while True:
            submission = self.__queue.get(block=True)
            if submission is not None:
                # print(submission)
                continue
            else:
                return

class WorkerDedicatedQueueSwarm:
    def enqueue(self, job):
        self.__queues[self.__round_robin].put(job)
        self.__round_robin = (self.__round_robin + 1) % len(self.__queues)

    def __init__(self, number_of_workers):
        self.__queues = []
        self.__processes = []
        self.__round_robin = 0;
        for i in range(0, number_of_workers):
            q = multiprocessing.Queue()
            t = WorkerDedicatedQueueProcess(q)
            self.__processes.append(t)
            self.__queues.append(q)

    def start(self):
        for process in self.__processes:
            process.start()

    def wait(self):
        for process in self.__processes:
            process.join()

    def stop(self):
        for q in self.__queues:
            # q.empty()
            q.put(None, block=True)



class PoolDedicatedQueue():
    def __init__(self, threads_number):
        self.ws = WorkerDedicatedQueueSwarm(threads_number)

        def term(sig, frame):
            self.ws.stop()
            self.ws.wait()

            if self.__prev_handler != 0:
                self.__prev_handler(sig, frame)

            os._exit(0)

        self.ws.start()

        self.__prev_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, term)

    def submit(self, job):
        self.ws.enqueue(job)

    def join(self):
        self.ws.stop()
        self.ws.wait()

###################################################

def fn(job):
    return

class PoolPython():
    def __init__(self, threads_number):
        self.pool = multiprocessing.Pool(processes=threads_number)

    def submit(self, job):
        res = self.pool.apply_async(fn, (job, ))

    def join(self):
        self.pool.close()
        self.pool.join()


####################################################


class WorkerZeroMQProcess(multiprocessing.Process):
    def __init__(self, i):
        super().__init__()
        self.index = i
        self.count = 0

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.connect("tcp://127.0.0.1:5557")

        while True:
            submission = socket.recv().decode('utf-8')
            if submission != 'None':
                # print(submission)
                self.count += 1
                continue
            else:
                socket.disconnect("tcp://127.0.0.1:5557")
                return

class WorkerZeroMQSwarm:
    def enqueue(self, job):
        # print(job)
        self.socket.send_string(job)

    def __init__(self, number_of_workers):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.bind("tcp://127.0.0.1:5557")

        self.__processes = []
        for i in range(0, number_of_workers):
            t = WorkerZeroMQProcess(i)
            self.__processes.append(t)

    def start(self):
        for process in self.__processes:
            process.start()

    def wait(self):
        for process in self.__processes:
            process.join()

    def stop(self):
        # self.__queue.empty()
        for process in self.__processes:
            self.socket.send_string('None')
        self.socket.disconnect("tcp://127.0.0.1:5557")



class PoolZeroMQ():
    def __init__(self, threads_number):
        self.ws = WorkerZeroMQSwarm(threads_number)

        def term(sig, frame):
            self.ws.stop()
            self.ws.wait()

            if self.__prev_handler != 0:
                self.__prev_handler(sig, frame)

            os._exit(0)

        self.ws.start()

        self.__prev_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, term)

    def submit(self, job):
        self.ws.enqueue(job)

    def join(self):
        self.ws.stop()
        self.ws.wait()


from time import sleep
def test_runner(text, text_size, pool_size):
    pool = PoolZeroMQ(pool_size)
    sleep(3)
    start = time()
    for i in range(100000):
        pool.submit(text)
    pool.join()
    end = time()
    print(f'100\'000 requests | ZeroMQ | Text size: {text_size} | Pool size: {pool_size} | Time: {round(end-start, 4)}s | Req/s: {int(100000 / (end-start))} [{int(100000 / (end-start) / 1000)}k]')

    pool = PoolSharedQueue(pool_size)
    start = time()
    for i in range(100000):
        pool.submit(text)
    pool.join()
    end = time()
    print(f'100\'000 requests | SharedQueue | Text size: {text_size} | Pool size: {pool_size} | Time: {round(end-start, 4)}s | Req/s: {int(100000 / (end-start))} [{int(100000 / (end-start) / 1000)}k]')

    pool = PoolDedicatedQueue(pool_size)
    start = time()
    for i in range(100000):
        pool.submit(text)
    pool.join()
    end = time()
    print(f'100\'000 requests | DedicatedQueue | Text size: {text_size} | Pool size: {pool_size} | Time: {round(end-start, 4)}s | Req/s: {int(100000 / (end-start))} [{int(100000 / (end-start) / 1000)}k]')

    pool = PoolPython(pool_size)
    start = time()
    for i in range(100000):
        pool.submit(text)
    pool.join()
    end = time()
    print(f'100\'000 requests | PoolPython | Text size: {text_size} | Pool size: {pool_size} | Time: {round(end-start, 4)}s | Req/s: {int(100000 / (end-start))} [{int(100000 / (end-start) / 1000)}k]')


if __name__ == '__main__':
    test_runner('a', '1B', 100)
    for pool_size in [1, 5, 10, 50]:
        for text_size in [(1, '1B'), (1024, '1KB'), (1024*1024, '1MB')]:
            text = 'a' * text_size[0]
            test_runner(text, text_size[1], pool_size)
