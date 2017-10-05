from monitor import Monitor
from threading import Thread
from time import sleep


class Buffer(Monitor):
    def __init__(self, id):
        self.size = 5
        self.buffer = []
        super().__init__(id, data=self.buffer)


class Producer(Thread):
    def __init__(self, id):
        super().__init__()
        self.buf = Buffer(id)

    def run(self):
        sleep(1)
        for i in range(0, 100):
            self.buf.request_cs()
            while len(self.buf.get()) >= self.buf.size:
                self.buf.wait()
            data = self.buf.get()
            print('put', i)
            data.append(i)
            self.buf.set(data)
            self.buf.leave_cs()
            self.buf.pulse_all()


class Consumer(Thread):
    def __init__(self, id):
        super().__init__()
        self.b = Buffer(id)

    def run(self):
        sleep(1)
        while True:
            self.b.request_cs()
            while len(self.b.get()) <= 0:
                self.b.wait()
            data = self.b.get()
            print('\tget', data.pop())
            self.b.set(data)
            # sleep(0.01)
            self.b.leave_cs()
            self.b.pulse_all()


Producer(0).start()
Consumer(1).start()
Producer(2).start()
Consumer(3).start()
Consumer(4).start()
