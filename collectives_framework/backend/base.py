from abc import ABC, abstractmethod


class Backend(ABC):
    def __init__(self, latency_model=None):
        self.latency = latency_model
        self.send_count = 0
        self.recv_count = 0

    @abstractmethod
    def size(self):
        pass

    @abstractmethod
    def pid(self):
        pass

    @abstractmethod
    def send(self, dst, data):
        pass

    @abstractmethod
    def recv(self, src):
        pass

    @abstractmethod
    def barrier(self):
        pass

    def reset_counters(self):
        self.send_count = 0
        self.recv_count = 0

