# -*- coding: utf-8 -*-

from typing import Callable, List
import gevent
import gevent.server
import gevent.lock
from gevent.socket import socket, create_connection

from scuttlebutt.asynclib.abstract import AbstractAsync, AbstractUdpServer, AbstractTcpServer, AbstractTcpConnection, AbstractLock, AbstractPeriodicFunction, AbstractCoroutine

class GeventUdpServer(AbstractUdpServer):
    def __init__(self, host: str, port: int, on_data_received: Callable):
        self._server = gevent.server.DatagramServer((host, port), self._data_received)
        self._on_data_received = on_data_received

    def _data_received(self, data, addr):
        self._on_data_received(data, addr)
    
    def start(self):
        self._server.start()

    def stop(self):
        self._server.stop()

    def send_to(self, data, host: str, port: int):
        self._server.sendto(data, (host, port))

class GeventTcpConnection(AbstractTcpConnection):
    def __init__(self, sock: socket):
        self._sock = sock
    
    def read(self, size) -> bytes:
        data = b''
        while len(data) < size:
            buffer = self._sock.recv(size - len(data))
            if buffer == '':
                break
            else:
                data += buffer
        return data

    def write(self, data):
        self._sock.send(data)

    def close(self):
        self._sock.close()

class GeventTcpServer(AbstractTcpServer):
    def __init__(self, host: str, port: int, on_connection_established: Callable, tcp_timeout: int = None):
        self._server = gevent.server.StreamServer((host, port), self._handle_tcp_connection)
        self._on_connection_established = on_connection_established
        self._tcp_timeout = tcp_timeout
    
    def _handle_tcp_connection(self, sock: socket, address):
        if self._tcp_timeout:
            sock.settimeout(self._tcp_timeout)
        self._on_connection_established(GeventTcpConnection(sock), address)
    
    def start(self):
        self._server.start()

    def stop(self):
        self._server.stop()

class GeventLock(AbstractLock):
    def __init__(self):
        self._lock = gevent.lock.Semaphore()

    def acquire(self, timeout: float = None) -> bool:
        return self._lock.acquire(timeout=timeout)

    def release(self):
        self._lock.release()

class GeventPeriodicFunction(AbstractPeriodicFunction):
    def __init__(self, interval: float, func: Callable, *args, **kw_args):
        self._interval = interval
        self._func = func
        self._func_args = args
        self._func_kw_args = kw_args
        self._timer = None

    def start(self):
        if self._timer:
            return
        else:
            def loop():
                while True:
                    gevent.spawn_later(0, self._func, *self._func_args, **self._func_kw_args)
                    gevent.sleep(self._interval)
            self._timer = gevent.spawn(loop)

    def stop(self):
        if self._timer:
            self._timer.kill()
        else:
            return

class GeventCoroutine(AbstractCoroutine):
    def __init__(self, greenlet):
        self.greenlet = greenlet
    
    def kill(self):
        self.greenlet.kill()
    
    def get_value(self):
        return self.greenlet.value

class GeventAsync(AbstractAsync):
    @staticmethod
    def run_forever():
        gevent.wait()
    
    @staticmethod
    def create_udp_server(host: str, port: int, on_data_received: Callable) -> GeventUdpServer:
        return GeventUdpServer(host, port, on_data_received)

    @staticmethod
    def create_tcp_server(host: str, port: int, on_connection_established: Callable, tcp_timeout: int = None) -> GeventTcpServer:
        return GeventTcpServer(host, port, on_connection_established, tcp_timeout)
    
    @staticmethod
    def create_tcp_connection(host: str, port: int, tcp_connection_timeout: int = None, tcp_timeout: int = None) -> GeventTcpConnection:
        sock = create_connection(address=(host, port), timeout=tcp_connection_timeout) # type: socket
        if tcp_timeout:
            sock.settimeout(tcp_timeout)
        return GeventTcpConnection(sock)
    
    @staticmethod
    def create_lock() -> GeventLock:
        return GeventLock()

    @staticmethod
    def create_periodic_function(interval: float, func: Callable, *args, **kw_args) -> GeventPeriodicFunction:
        return GeventPeriodicFunction(interval, func, *args, **kw_args)
    
    @staticmethod
    def run_in_coroutine(func: Callable, *args, **kw_args) -> GeventCoroutine:
        return GeventCoroutine(gevent.spawn(func, *args, **kw_args))
    
    @staticmethod
    def wait_all_couroutines(coroutines: List[GeventCoroutine]):
        gevent.joinall([ coroutine.greenlet for coroutine in coroutines ])
