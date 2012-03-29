This repository contains the ZeroMQ RPC driver for OpenStack Nova.

This driver supports brokerless communications between systems with an
asynchronous pattern.

Installation:
-------------

Run:
 python setup.py install

Set --rpc_backend=cloudscaling.nova.rpc.impl_zmq

Configuration:
--------------
While this driver supports brokerless messaging, there is an option to use a
broker. Thus, you can configure it in a brokerless or brokered pattern.

It is recommended to use the MatchMakerFanoutRing option for brokerless
messaging. For this, all nodes in the deployment should be present in a static
ring file.

MatchMaker Ring file format:
----------------------------

The MatchMaker Ring is a JSON blob containing a hash of each Nova topic, mapped
to an array of each host running the left-side service.

The hosts can be specified as hostnames, FQDNs, or as IP addresses. If
specifying hostnames of FQDNs, then host resolution through the system resolver
should be functional. Host resolution could happen through DNS or NetBIOS, for
example. Mapping to IP addresses is the easiest and most full-proof solution.

The hosts specified here should match those passed as the "--host" flag. By
default, Nova will set "--host" to the system's hostname. You may choose to pass
"--host" to each machine with the system's FQDN or IP address and set that value
in the ring file.

{
 "scheduler": [ "o1p409", "o2p409" ],
 "network": [ "o2p409", "o3p409" ],
 "cert": [ "o4p409", "o5p409" ],
 "vsa": [ "o6p409", "o7p409" ],
 "compute": [ "c1p509", "c2p509" ],
 "volume": [ "c1p509", "c2p509" ]
}

Notes on how it uses ZeroMQ:
----------------------------
Systems PUSH and PULL messages. Replies are sent as new messages with a request
to process (as a reply).

This pure PUSH/PULL pattern is more robust with less complexity compared to the
(X)REQ/(X)REP patterns.

Authors
-------
A full list of authors that have left their mark on this software is
included in a file called AUTHORS.
