======
Scuttlebutt
======

Scuttlebutt is a library that maintains a list of peers in a cluster using a gossip protocol inspired by SWIM.

Different techniques are used to improve over the SWIM protocol:
 - Infection-style communication using UDP instead of multicast.
 - Suspicion mechanism for the failure detection to reduce the false positives.
 - Round-robin probing allows the probing of all peers in a bounded time.
 - Regular full synchronisation between peers to reduce convergence time.

References
==========
 - https://prakhar.me/articles/swim/
 - https://www.brianstorti.com/swim/
 - https://www.serf.io/docs/internals/gossip.html

Dependencies
============

- `gevent <https://github.com/gevent/gevent>`_
