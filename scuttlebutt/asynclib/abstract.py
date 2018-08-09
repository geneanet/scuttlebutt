# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from typing import Callable, List

class AbstractUdpServer(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def send_to(self, data, host: str, port: int):
        pass

class AbstractTcpServer(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

class AbstractTcpConnection(ABC):
    @abstractmethod
    def read(self, size) -> bytes:
        pass

    @abstractmethod
    def write(self, data):
        pass

    @abstractmethod
    def close(self):
        pass

class AbstractLock(ABC):
    @abstractmethod
    def acquire(self, timeout: float) -> bool:
        pass
    
    @abstractmethod
    def release(self):
        pass

class AbstractPeriodicFunction(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

class AbstractCoroutine(ABC):
    @abstractmethod
    def kill(self):
        pass
    
    @abstractmethod
    def get_value(self):
        pass

class AbstractAsync(ABC):
    @staticmethod
    @abstractmethod
    def run_forever():
        pass

    @staticmethod
    @abstractmethod
    def create_udp_server(host: str, port: int, on_data_received: Callable) -> AbstractUdpServer:
        pass

    @staticmethod
    @abstractmethod
    def create_tcp_server(host: str, port: int, on_connection_established: Callable, tcp_timeout: int = None) -> AbstractTcpServer:
        pass

    @staticmethod
    @abstractmethod
    def create_tcp_connection(host: str, port: int, tcp_connection_timeout: int = None, tcp_timeout: int = None) -> AbstractTcpConnection:
        pass

    @staticmethod
    @abstractmethod
    def create_lock() -> AbstractLock:
        pass

    @staticmethod
    @abstractmethod
    def create_periodic_function(interval: float, func: Callable, *args, **kw_args) -> AbstractPeriodicFunction:
        pass

    @staticmethod
    @abstractmethod
    def run_in_coroutine(func: Callable, *args, **kw_args) -> AbstractCoroutine:
        pass
    
    @staticmethod
    @abstractmethod
    def wait_all_couroutines(coroutines: List[AbstractCoroutine]):
        pass

