# -*- coding: utf-8 -*-

from enum import Enum
from datetime import datetime

class PeerState(Enum):
    ALIVE           = 1
    SUSPECTED_DEAD  = 2
    DEAD            = 3

    def __str__(self):
        return self._name_

class Peer(object):
    def __init__(self, host: str, port: int, nodename: str = None):
        self.nodename = nodename
        self.host = host
        self.port = port

    def __eq__(self, other):
        return isinstance(other, Peer) and self.host == other.host and self.port == other.port
    
    def __lt__(self, other):
        if not isinstance(other, Peer):
            raise TypeError()
        return self.host < other.host or (self.host == other.host and self.port < other.port)

    def __gt__(self, other):
        if not isinstance(other, Peer):
            raise TypeError()
        return self.host > other.host or (self.host == other.host and self.port > other.port)
    
    def __hash__(self):
        return hash((self.host, self.port))

    def __repr__(self):
        return "<Peer %s:%d (%s))>" % (self.host, self.port, self.nodename)

class PeerUpdate(object):
    def __init__(self, peer: Peer, state: PeerState, refresh_timestamp: datetime = None):
        self.peer = peer
        self.state = state
        if refresh_timestamp == None:
            self.refresh_timestamp = datetime.utcnow()
        else:
            self.refresh_timestamp = refresh_timestamp
        self.state_change_timestamp = self.refresh_timestamp

    def __hash__(self):
        return hash((self.peer, self.state))
    
    def __eq__(self, other):
        return isinstance(other, PeerUpdate) and self.peer == other.peer and self.state == other.state

    def __lt__(self, other):
        if not isinstance(other, PeerUpdate):
            raise TypeError
        return self.peer < other.peer or (self.peer == other.peer and self.state < other.state)

    def __gt__(self, other):
        if not isinstance(other, PeerUpdate):
            raise TypeError
        return self.peer > other.peer or (self.peer == other.peer and self.state > other.state)

    def __repr__(self):
        return "<PeerUpdate %s: %s since %s at %s>" % (self.peer.nodename, self.state, self.state_change_timestamp, self.refresh_timestamp)
    
    def update_state(self, state: PeerState = PeerState.ALIVE, refresh_timestamp: datetime = None) -> None:
        if refresh_timestamp == None:
            refresh_timestamp = datetime.utcnow()
        
        if self.state == PeerState.DEAD and state == PeerState.SUSPECTED_DEAD:
            return
        
        if self.state != state:
            self.state_change_timestamp = refresh_timestamp

        self.state = state
        self.refresh_timestamp = refresh_timestamp
