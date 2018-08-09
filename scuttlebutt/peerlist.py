# -*- coding: utf-8 -*-

from typing import List, Callable, Iterable, Iterator
import random
import logging
import hashlib

from scuttlebutt.peer import Peer, PeerState, PeerUpdate
from scuttlebutt.rod import RandomlyOrderedDict

logger = logging.getLogger('scuttlebutt') # type: logging.Logger

class PeerList(object):
    def __init__(self):
        self._peers_state = RandomlyOrderedDict() # type: RandomlyOrderedDict[PeerUpdate]
        self._on_state_change_callbacks = [] # type: List[Callable]

    def __getitem__(self, peer: Peer) -> Peer:
        return self._peers_state[peer].peer

    def __delitem__(self, peer: Peer) -> None:
        if peer in self._peers_state:
            peer_state = self._peers_state[peer]
            del self._peers_state[peer]
            self._trigger_on_state_change_callbacks(peer_state.peer, None, peer_state.state)
        else:
            raise KeyError()
    
    def __len__(self):
        return len(self._peers_state)

    def __iter__(self):
        return self._peers_state.__iter__()

    def __repr__(self):
        return "<PeerList %s>" % (' '.join('%s[%s]' % (peer_state.peer.nodename, peer_state.state) for peer_state in self._peers_state.values()))

    def on_state_change(self, callback: Callable):
        self._on_state_change_callbacks.append(callback)

    def _trigger_on_state_change_callbacks(self, peer, state, oldstate):
        for callback in self._on_state_change_callbacks:
            callback(peer, state, oldstate)

    def set_peer_state(self, peer: Peer, state: PeerState):
        if peer in self._peers_state:
            old_state = self._peers_state[peer].state
            self._peers_state[peer].update_state(state)
        else:
            old_state = None
            self._peers_state[peer] = PeerUpdate(peer, state)

        if old_state != self._peers_state[peer].state:
            self._trigger_on_state_change_callbacks(peer, state, old_state)

    def apply_updates(self, updates: List[PeerUpdate]):
        # Ensure updates is a list
        if not isinstance(updates, Iterable):
            updates = [updates]

        for update in updates:
            if update.peer not in self._peers_state:
                self.set_peer_state(update.peer, update.state)
            elif update.refresh_timestamp >= self._peers_state[update.peer].refresh_timestamp:
                current = self._peers_state[update.peer]
                if update.state != current.state:
                    self.set_peer_state(update.peer, update.state)
                if current.peer.nodename != update.peer.nodename:
                    logger.warning("Node %s (%s:%d) changed name to %s", current.peer.nodename, current.peer.host, current.peer.port, update.peer.nodename)
                    current.peer.nodename = update.peer.nodename
                current.refresh_timestamp = update.refresh_timestamp

    def get_peers_states(self, exclude_peers: List[Peer] = None, include_states: Iterable[PeerState] = None):
        if exclude_peers == None:
            exclude_peers = []
        if include_states == None:
            include_states = list(PeerState)

        peers_states = list(
            peer_state
            for peer_state
            in self._peers_state.values()
            if peer_state.peer not in exclude_peers
            and peer_state.state in include_states
            )

        return peers_states

    def get_peers(self, exclude_peers: List[Peer] = None, include_states: Iterable[PeerState] = None) -> List[Peer]:
        return list(peer_state.peer for peer_state in self.get_peers_states(exclude_peers, include_states))
    
    def get_random_peers(self, count: int, **kwargs) -> List[Peer]:
        peers = self.get_peers(**kwargs)
        return random.sample(peers, min(len(peers), count))

    def cycle_peers(self, exclude_peers: List[Peer] = None, include_states: Iterable[PeerState] = None) -> Iterator[Peer]:
        i = iter(self._peers_state.values())
        nothing_matched = True

        while True:
            try:
                peer_state = next(i)
                if peer_state.peer in exclude_peers:
                    continue
                elif peer_state.state not in include_states:
                    continue
                else:
                    nothing_matched = False
                    yield peer_state.peer
            except StopIteration:
                if nothing_matched:
                    yield None

                i = iter(self._peers_state.values())
                nothing_matched = True

    def get_hash(self):
        sorted_list = sorted(list(self._peers_state.values()))
        string = ','.join('%s:%d=%s' % (peer_state.peer.host, peer_state.peer.port, peer_state.state) for peer_state in sorted_list)
        return hashlib.sha1(string.encode('ascii')).hexdigest()
