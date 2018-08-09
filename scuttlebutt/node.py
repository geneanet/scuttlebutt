# -*- coding: utf-8 -*-

import logging
import pickle
from typing import Dict, List, Callable, Tuple
from uuid import uuid1, UUID
from datetime import datetime, timedelta
from socket import gethostname
import struct

from scuttlebutt.peer import Peer, PeerState, PeerUpdate
from scuttlebutt.peerlist import PeerList
from scuttlebutt import messages
from scuttlebutt.asynclib import AbstractAsync, AbstractTcpConnection, AbstractLock, AbstractCoroutine, AbstractPeriodicFunction, GeventAsync

PROTOCOL_HELLO = b'\x50\x6e\x78\x3c\x73\x4c\x5a\x91'
PROTOCOL_VERSION = 1

logger = logging.getLogger('scuttlebutt') # type: logging.Logger
net_logger = logging.getLogger('scuttlebutt.network') # type: logging.Logger

class Node(object):
    def __init__(self,
                 nodename: str = None,
                 host: str = '127.0.0.1',
                 port: int = 8642,
                 advertised_host: str = None,
                 advertised_port: int = None,
                 async_lib: AbstractAsync = GeventAsync,
                 gossip_interval: float = 0.2,
                 gossip_fanout: int = 3,
                 probe_interval: float = 1,
                 indirect_probes: int = 3,
                 ping_timeout: float = .5,
                 indirect_ping_timeout: float = .7,
                 death_suspicion_timeout: float = 30,
                 death_timeout: float = 86400,
                 cleanup_interval: float = 1,
                 full_update_interval: float = 30,
                 tcp_connection_timeout: float = 5,
                 tcp_timeout: float = 10,
                 gossip_message_max_updates: int = 10,
                 revive_dead_interval: float = 30
                 ):
        if nodename:
            self.nodename = nodename
        else:
            self.nodename = gethostname()

        self.host = host
        self.port = port
        if advertised_host:
            self.advertised_host = advertised_host
        else:
            self.advertised_host = host
        if advertised_port:
            self.advertised_port = advertised_port
        else:
            self.advertised_port = port

        self.async_lib = async_lib

        self.gossip_interval = gossip_interval
        self.gossip_fanout = gossip_fanout
        self.probe_interval = probe_interval
        self.indirect_probes = indirect_probes
        self.ping_timeout = ping_timeout
        self.indirect_ping_timeout = indirect_ping_timeout
        self.death_suspicion_timeout = death_suspicion_timeout
        self.death_timeout = death_timeout
        self.cleanup_interval = cleanup_interval
        self.full_update_interval = full_update_interval
        self.tcp_connection_timeout = tcp_connection_timeout
        self.tcp_timeout = tcp_timeout
        self.gossip_message_max_updates = gossip_message_max_updates
        self.revive_dead_interval = revive_dead_interval

        self._self_peer = Peer(nodename=self.nodename, host=self.advertised_host, port=self.advertised_port)
        self._udp_server = self.async_lib.create_udp_server(self.host, self.port, self._handle_udp_datagram)
        self._tcp_server = self.async_lib.create_tcp_server(self.host, self.port, self._handle_tcp_connection, tcp_timeout=self.tcp_timeout)
        self.peerlist = PeerList()
        self._probe_peer_iterator = self.peerlist.cycle_peers(exclude_peers=[self._self_peer], include_states=[PeerState.ALIVE, PeerState.SUSPECTED_DEAD])
        self._gossip_timer = None # type: AbstractPeriodicFunction
        self._probe_timer = None # type: AbstractPeriodicFunction
        self._cleanup_timer = None # type: AbstractPeriodicFunction
        self._full_update_timer = None # type: AbstractPeriodicFunction
        self._revive_dead_timer = None # type: AbstractPeriodicFunction
        self._locks = {} # type: Dict[UUID, AbstractLock]
        self._gossip_buffer = {} # type: Dict[Peer,PeerUpdate]
        self._on_state_change = []  # type: List[Callable]

        self.peerlist.on_state_change(self._on_peer_state_changed)

    def on_state_change(self, callback: Callable):
        self.peerlist.on_state_change(callback)
    
    def _on_peer_state_changed(self, peer: Peer, state: PeerState, oldstate: PeerState): #pylint: disable=unused-argument
        if state == None:
            if peer in self._gossip_buffer:
                del self._gossip_buffer[peer]
        else:
            self._gossip_buffer[peer] = PeerUpdate(peer, state)

    def start(self):
        self.peerlist.set_peer_state(self._self_peer, PeerState.ALIVE)
        self._udp_server.start()
        self._tcp_server.start()
        self.prove_alive()
        if self.gossip_interval > 0 and self.gossip_fanout > 0:
            self._gossip_timer = self.async_lib.create_periodic_function(self.gossip_interval, self.gossip)
            self._gossip_timer.start()
        if self.probe_interval > 0:
            self._probe_timer = self.async_lib.create_periodic_function(self.probe_interval, self.probe_peer)
            self._probe_timer.start()
        if self.cleanup_interval > 0:
            self._cleanup_timer = self.async_lib.create_periodic_function(self.cleanup_interval, self.cleanup)
            self._cleanup_timer.start()
        if self.full_update_interval > 0:
            self._full_update_timer = self.async_lib.create_periodic_function(self.full_update_interval, self.full_update)
            self._full_update_timer.start()
        if self.revive_dead_interval > 0:
            self._revive_dead_timer = self.async_lib.create_periodic_function(self.revive_dead_interval, self.revive_dead)
            self._revive_dead_timer.start()
    
    def stop(self):
        self.peerlist.set_peer_state(self._self_peer, PeerState.DEAD)
        self.gossip()
        self._udp_server.stop()
        self._tcp_server.stop()
        if self._gossip_timer != None :
            self._gossip_timer.stop()
        if self._probe_timer != None :
            self._probe_timer.stop()
        if self._cleanup_timer != None :
            self._cleanup_timer.stop()
        if self._full_update_timer != None :
            self._full_update_timer.stop()
        if self._revive_dead_timer != None :
            self._revive_dead_timer.stop()

    def _handle_tcp_connection(self, cnx: AbstractTcpConnection, address: Tuple[str, int]):
        net_logger.debug('<- Received TCP connection from %s:%d', *address)

        self._read_tcp_header(cnx)
        message = self._read_tcp_message(cnx)
        
        try:
            if isinstance(message, messages.RequestUpdate):
                self._write_tcp_message(cnx, messages.Update(self._self_peer, list(self.peerlist.get_peers_states(include_states=[PeerState.ALIVE, PeerState.SUSPECTED_DEAD, PeerState.DEAD]))))
            else:
                raise TypeError("Unexpected message type %s" % type(message))
        except Exception as e:
            self._write_tcp_message(cnx, messages.Error(self._self_peer, str(e)))
            raise
        finally:
            cnx.close()

        if message.source not in self.peerlist:
            logger.info('Received a message from peer %s not in peerlist.', message.source.nodename)
            self.peerlist.set_peer_state(message.source, PeerState.ALIVE)

    def _query_tcp(self, peer: Peer, message: messages.Message) -> messages.Message:
        cnx = self.async_lib.create_tcp_connection(peer.host, peer.port, self.tcp_connection_timeout, self.tcp_timeout)

        self._write_tcp_header(cnx)
        self._write_tcp_message(cnx, message)

        received_message = self._read_tcp_message(cnx)

        return received_message

    @staticmethod
    def _write_tcp_header(cnx: AbstractTcpConnection):
        cnx.write(PROTOCOL_HELLO)
        cnx.write(struct.pack('!B',PROTOCOL_VERSION))

    @staticmethod
    def _read_tcp_header(cnx: AbstractTcpConnection):
        hello = cnx.read(len(PROTOCOL_HELLO))
        if hello != PROTOCOL_HELLO:
            raise Exception('Bad protocol Hello.')

        version = cnx.read(1)
        if len(version) < 1:
            raise Exception('Protocol error: can\'t read version')
        (version,) = struct.unpack('!B', version)
        if version != PROTOCOL_VERSION:
            raise Exception('Protocol version mismatch (received: %d, ours: %d)' % (version, PROTOCOL_VERSION))

    @staticmethod
    def _write_tcp_message(cnx: AbstractTcpConnection, message: messages.Message):
        data = pickle.dumps(message)
        cnx.write(struct.pack('!I', len(data)))
        cnx.write(data)
        net_logger.debug('-> Sent TCP message : %s', message)

    @staticmethod
    def _read_tcp_message(cnx: AbstractTcpConnection) -> messages.Message:
        size = cnx.read(4)
        if len(size) < 4:
            raise Exception('Protocol error: can\'t read message size')
        (size,) = struct.unpack('!I', size)

        message = cnx.read(size)
        if len(message) < size:
            raise Exception('Protocol error: can\'t read message')
        
        message = pickle.loads(message)
        if not isinstance(message, messages.Message):
            raise Exception('Protocol error: invalid message')

        net_logger.debug('<- Received TCP message from %s : %s', message.source, message)

        return message

    def _handle_udp_datagram(self, data: bytes, address: Tuple[str, int]):
        try:
            message = pickle.loads(data)
            if not isinstance(message, messages.Message):
                raise Exception('Protocol error: invalid message')

            net_logger.debug('<- Received message from %s : %s', message.source, message)

            if isinstance(message, messages.Update):
                self.peerlist.apply_updates(self.filter_fake_news(message.updates))
            elif isinstance(message, messages.PingReq):
                if self.ping(message.target):
                    self.send_message_pong(message.source, message.uuid)
            elif isinstance(message, messages.Ping):
                self.send_message_pong(message.source, message.uuid)
            elif isinstance(message, messages.Pong):
                if message.ping_uuid in self._locks:
                    self._locks[message.ping_uuid].release()
            else:
                raise TypeError("Unexpected message type %s" % type(message))

        except Exception as e:
            logger.error("Error while receiving message from %s:%d (%s).", *address, e)
            raise

        if message.source not in self.peerlist:
            logger.info('Received a message from peer %s not in peerlist.', message.source.nodename)
            self.peerlist.set_peer_state(message.source, PeerState.ALIVE)

    def prove_alive(self):
        self._gossip_buffer[self._self_peer] = PeerUpdate(self._self_peer, PeerState.ALIVE)

    def gossip(self):
        if len(self._gossip_buffer):
            peers = self.peerlist.get_random_peers(count=self.gossip_fanout, exclude_peers=[self._self_peer], include_states=[PeerState.ALIVE])
            if len(peers):
                buffer = []
                for node in list(self._gossip_buffer.keys())[:self.gossip_message_max_updates]:
                    buffer.append(self._gossip_buffer[node])
                    del self._gossip_buffer[node]
                data = pickle.dumps(messages.Update(self._self_peer, buffer))
                logging.debug("Gossiping with %s (%s)", peers, buffer)
                for peer in peers:
                    self._udp_server.send_to(data, peer.host, peer.port)

    def probe_peer(self):
        probed_peer = next(self._probe_peer_iterator)

        if probed_peer != None:
            logger.debug("Probing peer %s", probed_peer.nodename)
        else:
            logger.debug("No peer to probe")
            return

        if self.ping(probed_peer):
            logger.debug("Peer %s is reported alive after direct ping.", probed_peer.nodename)
            self.peerlist.set_peer_state(probed_peer, PeerState.ALIVE)
        else:
            peers = self.peerlist.get_random_peers(count=self.indirect_probes, exclude_peers=[probed_peer, self._self_peer], include_states=[PeerState.ALIVE])
            logger.warning("Peer %s did not answer to direct ping. Sending %d indirect pings via peers %s", probed_peer.nodename, self.indirect_probes, ','.join(peer.nodename for peer in peers))
            ping_threads = [] # type: List[AbstractCoroutine]
            for via in peers:
                ping_threads.append(self.async_lib.run_in_coroutine(self.ping, probed_peer, via))
            self.async_lib.wait_all_couroutines(ping_threads)
            if any(thread.get_value() for thread in ping_threads):
                logger.debug("Peer %s is reported alive after indirect ping.", probed_peer.nodename)
                self.peerlist.set_peer_state(probed_peer, PeerState.ALIVE)
            else:
                logger.warning("Peer %s did not answer to indirect ping.", probed_peer.nodename)
                self.peerlist.set_peer_state(probed_peer, PeerState.SUSPECTED_DEAD)
        
    def full_update(self):
        peers = self.peerlist.get_random_peers(count=1, exclude_peers=[self._self_peer], include_states=[PeerState.ALIVE])
        if len(peers) == 1:
            peer = peers[0]
            logger.debug("Asking peer %s for a full update.", peer.nodename)
        else:
            logger.debug("No peer to ask for a full update.")
            return

        message = self._query_tcp(peer, messages.RequestUpdate(self._self_peer))
        assert isinstance(message, messages.Update)
        self.peerlist.apply_updates(self.filter_fake_news(message.updates))

    def revive_dead(self):
        peers = self.peerlist.get_peers(include_states=[PeerState.DEAD])
        
        if len(peers) == 0:
            logger.debug("No dead peers to revive.")
            return

        for peer in peers: # type: Peer
            logger.debug("Trying to revive dead peer %s.", peer.nodename)
            if self.ping(peer):
                self.peerlist.set_peer_state(peer, PeerState.ALIVE)

    def cleanup(self):
        peers_states = self.peerlist.get_peers_states(exclude_peers=[self._self_peer])
        for peer_state in peers_states: # type: PeerUpdate
            if peer_state.state == PeerState.SUSPECTED_DEAD and datetime.utcnow() > peer_state.state_change_timestamp + timedelta(seconds = self.death_suspicion_timeout):
                logging.warning('Peer %s suspected dead for more that %d seconds. Setting state to DEAD.', peer_state.peer.nodename, self.death_suspicion_timeout)
                self.peerlist.set_peer_state(peer_state.peer, PeerState.DEAD)
            elif peer_state.state == PeerState.DEAD and datetime.utcnow() > peer_state.state_change_timestamp + timedelta(seconds = self.death_timeout):
                logging.info('Removing dead peer %s from peer list.', peer_state.peer.nodename)
                del self.peerlist[peer_state.peer]

    def ping(self, peer: Peer, via: Peer = None) -> bool:
        ping_id = uuid1()
        lock = self.async_lib.create_lock()
        self._locks[ping_id] = lock
        lock.acquire()

        if via == None:
            self.send_message_ping(peer, ping_id)
            timeout = self.ping_timeout
        else:
            self.send_message_ping_req(via, peer, ping_id)
            timeout = self.indirect_ping_timeout

        if lock.acquire(timeout) == True: # The semaphore has been released
            del self._locks[ping_id]
            return True
        else: # The timeout has been reached
            del self._locks[ping_id]
            return False
        
    def send_message_ping(self, peer: Peer, uuid: UUID):
        self._send_message(peer, messages.Ping(self._self_peer, uuid))

    def send_message_ping_req(self, peer: Peer, target: Peer, uuid: UUID):
        self._send_message(peer, messages.PingReq(self._self_peer, target, uuid))

    def send_message_pong(self, peer: Peer, ping_uuid: UUID):
        self._send_message(peer, messages.Pong(self._self_peer, ping_uuid))

    def _send_message(self, peer: Peer, message: messages.Message):
        net_logger.debug('-> Sending message to %s : %s', peer, message)
        self._udp_server.send_to(pickle.dumps(message), peer.host, peer.port)

    def bootstrap(self, host: str, port: int):
        message = self._query_tcp(Peer(host=host, port=port), messages.RequestUpdate(self._self_peer))
        assert isinstance(message, messages.Update)
        self.peerlist.apply_updates(self.filter_fake_news(message.updates))

    def filter_fake_news(self, updates: List[PeerUpdate]):
        filtered_updates = []
        for update in updates:
            # Nobody can speak on our name !
            if update.peer == self._self_peer:
                # If somebody pretends we are dead, spread the news that we are alive
                if update.state != PeerState.ALIVE:
                    self.prove_alive()
            else:
                filtered_updates.append(update)
        return filtered_updates