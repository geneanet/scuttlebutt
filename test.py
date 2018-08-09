#!/usr/bin/python3
# -*- coding: utf-8 -*-

import scuttlebutt
import argparse
import logging
from scuttlebutt.asynclib import GeventAsync

logging.basicConfig(level=logging.DEBUG, format =  '%(asctime)s [%(process)d:%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger('scuttlebutt') # type: logging.Logger
logging.getLogger('scuttlebutt.network').setLevel(logging.INFO)

def parse_address(address):
    if isinstance(address, str):
        address = address.split(':')
        if len(address) == 1:
            return (address[0], 8642)
        elif len(address) == 2:
            return (address[0], int(address[1]))
        else:
            raise ValueError('address should be a tuple (host, port) or a string "host" or "host:port"')
    elif isinstance(address, tuple):
        if len(address) == 2:
            return address
        else:
            raise ValueError('address should be a tuple (host, port) or a string "host" or "host:port"')
    else:
        raise TypeError()

def peer_update(peer: scuttlebutt.Peer, state: scuttlebutt.PeerState, old_state: scuttlebutt.PeerState):
    if old_state == None:
        logger.info('New peer %s status %s.', peer.nodename, state)
    elif state == None:
        logger.info('Peer %s has been removed.', peer.nodename)
    else:
        logger.info('Peer %s status changed from %s to %s.', peer.nodename, old_state, state)

    logger.info("Peer List : %s (hash %s)", node.peerlist, node.peerlist.get_hash())
  
parser = argparse.ArgumentParser()
parser.add_argument('--bind', default='127.0.0.1:8642', help='Bind host[:port] (default: %(default)s)')
parser.add_argument('--nodename', default=None, help='Node name (default: hostname)')
parser.add_argument('--bootstrap', nargs='+', default=[], help='Peers to contact (format host[:port])')
args = parser.parse_args()

(host, port) = parse_address(args.bind)

async_lib = GeventAsync()

node = scuttlebutt.Node(args.nodename, host, port, async_lib = async_lib, death_suspicion_timeout=5, death_timeout=86400, revive_dead_interval=20)
node.on_state_change(peer_update)
node.start()
for bootstrap_peer in args.bootstrap:
    (peer_host, peer_port) = parse_address(bootstrap_peer)
    node.bootstrap(peer_host, peer_port)

try:
    async_lib.run_forever()
except (KeyboardInterrupt, SystemExit):
    logger.info('Termination requested.')
    node.stop()
    logger.info('Terminated.')
    exit(1)
