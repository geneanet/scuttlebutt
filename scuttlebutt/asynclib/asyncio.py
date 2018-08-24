# -*- coding: utf-8 -*-

from typing import Callable, List
import asyncio
import functools

from scuttlebutt.asynclib.abstract import AbstractAsync, AbstractUdpServer, AbstractTcpServer, AbstractTcpConnection, AbstractLock, AbstractPeriodicFunction, AbstractCoroutine

loop = asyncio.get_event_loop() # type: asyncio.BaseEventLoop

class _UdpServerProtocol(asyncio.protocols.DatagramProtocol):
    def __init__(self, on_data_received: Callable):
        self._on_data_received = on_data_received

    def datagram_received(self, data, addr):
        self._on_data_received(data, addr)

class AsyncioUdpServer(AbstractUdpServer):
    def __init__(self, host: str, port: int, on_data_received: Callable):
        self._on_data_received = on_data_received
        self._host = host
        self._port = port
        self._transport = None
        self._protocol = None

    def start(self):
        if not self._transport:
            def protocol_factory():
                return _UdpServerProtocol(self._on_data_received)
            listen = loop.create_datagram_endpoint(protocol_factory, local_addr=(self._host, self._port))
            self._transport, self._protocol = loop.run_until_complete(listen)

    def stop(self):
        if self._transport:
            self._transport.close()
            self._transport = self._protocol = None

    def send_to(self, data, host: str, port: int):
        self._transport.sendto(data, (host, port))

class AsyncioTcpConnection(AbstractTcpConnection):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
    
    async def read(self, size) -> bytes:
        return await self._reader.readexactly(size)

    def write(self, data):
        self._writer.write(data)

    def close(self):
        self._writer.write_eof()

class AsyncioTcpServer(AbstractTcpServer):
    def __init__(self, host: str, port: int, on_connection_established: Callable, tcp_timeout: int = None):
        self._host = host
        self._port = port
        self._on_connection_established = on_connection_established
        self._tcp_timeout = tcp_timeout
        self._server = None
    
    async def _handle_tcp_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        address = writer.get_extra_info('peername')
        sock = writer.get_extra_info('socket')
        if self._tcp_timeout:
            sock.settimeout(self._tcp_timeout)
        await self._on_connection_established(AsyncioTcpConnection(reader, writer), address)
    
    def start(self):
        if not self._server:
            listen = asyncio.start_server(self._handle_tcp_connection, self._host, self._port)
            self._server = loop.run_until_complete(listen)

    def stop(self):
        if self._server:
            self._server.close()
            self._server = None

class AsyncioLock(AbstractLock):
    def __init__(self):
        self._lock = asyncio.Lock()

    async def acquire(self, timeout: float = None) -> bool:
        acq_task = loop.create_task(self._lock.acquire())
        loop.call_later(timeout, acq_task.cancel)
        try:
            await acq_task
            return True
        except asyncio.CancelledError:
            return False

    def release(self):
        self._lock.release()

class AsyncioPeriodicFunction(AbstractPeriodicFunction):
    def __init__(self, interval: float, func: Callable, *args, **kw_args):
        self._interval = interval
        self._func = functools.partial(func, *args, **kw_args)
        self._timer = None

    def start(self):
        if not self._timer:
            async def timer_loop():
                while True:
                    if asyncio.iscoroutinefunction(self._func):
                        loop.create_task(self._func())
                    else:
                        loop.call_soon(self._func)
                    await asyncio.sleep(self._interval)

            self._timer = loop.create_task(timer_loop)

    def stop(self):
        if self._timer:
            self._timer.cancel()

class AsyncioCoroutine(AbstractCoroutine):
    def __init__(self, task: asyncio.Task):
        self.task = task
    
    def kill(self):
        self.task.cancel()
    
    def get_value(self):
        try:
            return self.task.result()
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            return None

class AsyncioAsync(AbstractAsync):
    @staticmethod
    def run_forever():
        loop.run_forever()
    
    @staticmethod
    def create_udp_server(host: str, port: int, on_data_received: Callable) -> AsyncioUdpServer:
        return AsyncioUdpServer(host, port, on_data_received)

    @staticmethod
    def create_tcp_server(host: str, port: int, on_connection_established: Callable, tcp_timeout: int = None) -> AsyncioTcpServer:
        return AsyncioTcpServer(host, port, on_connection_established, tcp_timeout)
    
    @staticmethod
    async def create_tcp_connection(host: str, port: int, tcp_connection_timeout: int = None, tcp_timeout: int = None) -> AsyncioTcpConnection:
        conn_task = loop.create_task(asyncio.open_connection(host, port))
        loop.call_later(tcp_connection_timeout, conn_task.cancel)
        reader, writer = await conn_task
        sock = writer.get_extra_info('socket')
        if tcp_timeout:
            sock.settimeout(tcp_timeout)
        return AsyncioTcpConnection(reader, writer)

    @staticmethod
    def create_lock() -> AsyncioLock:
        return AsyncioLock()

    @staticmethod
    def create_periodic_function(interval: float, func: Callable, *args, **kw_args) -> AsyncioPeriodicFunction:
        return AsyncioPeriodicFunction(interval, func, *args, **kw_args)
    
    @staticmethod
    def run_in_coroutine(func: Callable, *args, **kw_args) -> AsyncioCoroutine:
        async def coro():
            return func(*args, **kw_args)
        return AsyncioCoroutine(loop.create_task(coro))
    
    @staticmethod
    def wait_all_couroutines(coroutines: List[AsyncioCoroutine]):
        asyncio.wait([ coroutine.task for coroutine in coroutines ])
