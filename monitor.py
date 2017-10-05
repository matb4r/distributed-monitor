import pika
import pickle
from threading import Thread, Lock, Condition
from enum import Enum
from time import sleep

DEBUG = False
N = 5  # number of processes


class Token:
    def __init__(self):
        self.LN = [0] * N  # Last request Number
        self.Q = []


class MsgType(Enum):
    REQUEST = 1
    TOKEN = 2
    PULSE = 3
    SET = 4


class ProcessState(Enum):
    IDLE = 0
    WAITING_FOR_CS = 1
    IN_CS = 2
    IN_CS_WAITING = 3


class Message:
    def __init__(self, pid, msg_type, token, recipient, sn, data):
        self.pid = pid
        self.msg_type = msg_type
        self.token = token
        self.recipient = recipient
        self.sn = sn  # Sequence number
        self.data = data


class Monitor:
    def __init__(self, id, data=None):
        self.id = id
        self.state = ProcessState.IDLE
        self.token = Token() if id == 0 else None
        self.RN = [0] * N  # Request Number
        self.channel = None
        self.__amqp_init()
        self.__listen()
        self.lock = Lock()
        self.cv = Condition(self.lock)
        self.__data = data
        self.setter_delay = 0.1

    def __amqp_init(self):
        self.__debug('init')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='msgs', type='fanout')
        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='msgs', queue=queue_name)
        self.channel.basic_consume(self.__recv_callback, queue=queue_name, no_ack=True)

    def __listen(self):
        self.__debug('listening')
        Thread(target=self.channel.start_consuming).start()

    def __send_msg(self, msg_type, token=None, recipient=None, sn=None, data=None):
        msg = Message(self.id, msg_type, token, recipient, sn, data)
        self.channel.basic_publish(exchange='msgs', routing_key='', body=pickle.dumps(msg))

    def __recv_callback(self, ch, method, properties, body):
        msg = pickle.loads(body)

        if msg.msg_type == MsgType.REQUEST:
            self.__recv_request(msg)
        elif msg.msg_type == MsgType.TOKEN:
            self.__recv_token(msg)
        elif msg.msg_type == MsgType.PULSE:
            self.__recv_pulse(msg)
        elif msg.msg_type == MsgType.SET:
            self.__recv_data(msg)

    def __recv_request(self, msg):
        if msg.pid != self.id:
            self.__debug('received REQUEST from ' + str(msg.pid))

            if self.RN[msg.pid] < msg.sn:
                self.RN[msg.pid] = msg.sn
            elif self.RN[msg.pid] > msg.sn:
                self.__debug('but this REQUEST is outdated')

            if self.token and self.state != ProcessState.IN_CS and self.RN[msg.pid] == self.token.LN[msg.pid] + 1:
                self.__debug('sending TOKEN to ' + str(msg.pid))
                self.__send_msg(MsgType.TOKEN, token=self.token, recipient=msg.pid)
                self.token = None

    def __recv_token(self, msg):
        if self.id == msg.recipient:
            self.__debug('received TOKEN from ' + str(msg.pid))
            self.token = msg.token
            self.__enter_cs()

    def __recv_pulse(self, msg):
        if msg.recipient is None or msg.recipient is self.id:
            self.__debug('received pulse')
            with self.cv:
                self.cv.notify()
            if self.state == ProcessState.IN_CS_WAITING:
                self.request_cs()

    def __recv_data(self, msg):
        if msg.pid != self.id:
            self.__debug('received data from ' + str(msg.pid))
            self.__debug('setting data to ' + str(msg.data))
            self.__data = msg.data

    def __enter_cs(self):
        self.__debug('entering cs')
        if self.token:
            with self.cv:
                self.cv.notify()
            self.state = ProcessState.IN_CS
        else:
            self.__debug('but I do not have token...')

    def __debug(self, text):
        if DEBUG:
            print('\t' * self.id, self.id, text)

    def debug(self, text):
        self.__debug(text)

    def request_cs(self):
        self.__debug('requesting cs')

        if self.state == ProcessState.IN_CS:
            self.__debug('but I\'m already in cs')
        elif self.state == ProcessState.WAITING_FOR_CS:
            self.__debug('already waiting')
        else:
            self.state = ProcessState.WAITING_FOR_CS
            if self.token is None:
                self.RN[self.id] += 1
                self.__debug('sending REQUEST')
                self.__send_msg(MsgType.REQUEST, sn=self.RN[self.id])
                with self.cv:
                    self.cv.wait()
            else:
                self.__debug('but I have token already')
                self.__enter_cs()

    def leave_cs(self):
        self.__debug('releasing cs')
        if self.state != ProcessState.IN_CS:
            self.__debug('I\'m not in cs...')
        elif self.token is None:
            self.__debug('I do not have token...')
        else:
            self.state = ProcessState.IDLE
            self.token.LN[self.id] = self.RN[self.id]

            self.__debug('whos now?')
            for j in range(N):
                if j not in self.token.Q:
                    if self.RN[j] == self.token.LN[j] + 1:
                        self.token.Q.append(j)

            if self.token.Q:
                next_process = self.token.Q.pop(0)
                self.__debug(str(next_process) + '!')
                self.__debug('sending token to ' + str(next_process))
                self.__send_msg(MsgType.TOKEN, token=self.token, recipient=next_process)
                self.token = None
            else:
                self.__debug('nobody...')

    def wait(self):
        self.__debug('waiting')
        if self.state == ProcessState.IN_CS:
            self.leave_cs()
            self.state = ProcessState.IN_CS_WAITING
        with self.cv:
            self.cv.wait()

    def pulse(self, pid):
        self.__debug('pulsing ' + str(pid))
        self.__send_msg(MsgType.PULSE, recipient=pid)

    def pulse_all(self):
        self.__debug('pulsing all')
        self.__send_msg(MsgType.PULSE)

    def get(self):
        return self.__data

    def set(self, data):
        self.__debug('setting data to: ' + str(data))
        self.__data = data
        self.__debug('sending data')
        self.__send_msg(MsgType.SET, data=self.__data)
        sleep(self.setter_delay)
