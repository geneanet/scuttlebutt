# -*- coding: utf-8 -*-

from typing import List

from scuttlebutt.peer import Peer, PeerUpdate
from uuid import UUID

class Message(object):
    def __init__(self, source: Peer):
        self.source = source

class Ack(Message):
    pass

class Error(Message):
    def __init__(self, source: Peer, message: str):
        super().__init__(source)
        self.message = message

class Update(Message):
    def __init__(self, source: Peer, updates: List[PeerUpdate]):
        super().__init__(source)
        self.updates = updates

class RequestUpdate(Message):
    pass

class PingReq(Message):
    def __init__(self, source: Peer, target: Peer, uuid: UUID):
        super().__init__(source)
        self.target = target
        self.uuid = uuid

class Ping(Message):
    def __init__(self, source: Peer, uuid: UUID):
        super().__init__(source)
        self.uuid = uuid

class Pong(Message):
    def __init__(self, source: Peer, ping_uuid: UUID):
        super().__init__(source)
        self.ping_uuid = ping_uuid
