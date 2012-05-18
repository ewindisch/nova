# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2011 Cloudscaling Group, Inc
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import eventlet
from greenlet import GreenletExit
eventlet.monkey_patch()

import collections
import contextlib
import hashlib
import itertools
import json
import os
from pprint import pformat
import random
import socket
import string
import sys
import types
import traceback
import uuid

from nova import context
from nova import exception
from nova.openstack.common import cfg
from nova.rpc.common import RemoteError, LOG
from nova import flags
from nova import utils


contextmanager = contextlib.contextmanager

matchmaker_opts = [
    # Matchmaker ring file
    cfg.StrOpt('matchmaker_ringfile',
        default='/etc/nova/matchmaker_ring.json',
        help='Matchmaker ring file (JSON)'),
]

FLAGS = flags.FLAGS
FLAGS.register_opts(matchmaker_opts)


"""
The MatchMaker classes should except a Topic or Fanout exchange key and
return keys for direct exchanges, per (approximate) AMQP parlance.
"""

class MatchMakerException(exception.NovaException):
    """Signified a match could not be found."""
    message = _("Match not found by MatchMaker.")


class Exchange(object):
    """
    Implements lookups.
    Subclass this to support hashtables, dns, etc.
    """
    def __init__(self):
        pass

    def run(self, context, topic):
        raise NotImplementedError()


class Binding(object):
    """
    A binding on which to perform a lookup.
    """
    def __init__(self):
        pass


class MatchMakerBase(object):
    """Match Maker Base Class"""

    def __init__(self):
        # Array of tuples. Index [2] toggles negation
        self.bindings = []

    def add_binding(self, binding, rule, last=False):
        self.bindings.append((binding, rule, False, last))

    def add_negate_binding(self, binding, rule, last=False):
        self.bindings.append((binding, rule, True, last))

    def queues(self, context, topic):
        workers = []
        for (binding, exchange, bit, last) in self.bindings:
            #x = binding.run(context, topic)
            #if (bit and not x) or x:
            #   workers.extend(exchange.run(style, context, topic))
            with binding:
                workers.extend(exchange.run(context, topic))

            if len(workers) >= limit:
                return workers[0:limit]
        return workers


# Get a host on bare topics.
# Not needed for ROUTER_PUB which is always brokered.
class PassExchange(Exchange):
    def run(self, context, topic):
        return (context, topic)


class DirectTopicBinding(Binding):
    def __enter__(self, context, topic):
        if '.' in topic:
            return True
        return False


class BareTopicBinding(Binding):
    def __enter__(self, context, topic):
        if '.' not in topic:
            return True
        return False


# Get a host on bare topics.
# Not needed for ROUTER_PUB which is always brokered.
class FanoutBinding(Binding):
    def __enter__(self, context, topic):
        if topic.startswith('fanout.'):
            return True
        return False


class RingExchange(Exchange):
    """
    Match Maker where hosts are loaded from a static file
    """
    def __init__(self):
        super(RingExchange, self).__init__()

        fh = open(FLAGS.matchmaker_ringfile, 'r')
        self.ring = json.load(fh)
        self.ring0 = {}
        for k in self.ring.keys():
            self.ring0[k] = itertools.cycle(self.ring[k])
        fh.close()
        LOG.debug(_("RING:\n%s"), self.ring0)

    def next(self):
        return next(self.ring0[topic])

    def _has(self, topic):
        if topic in self.ring0:
        	return True
        return False


class RoundRobinRingExchange(RingExchange):
    """A Topic Exchange"""
    def __init__(self):
        super(RoundRobinRingExchange, self).__init__()

    def run(self, context, topic):
        if not self._has(topic):
            LOG.debug(
                _("No key defining hosts for topic '%(topic)', "
                  "see ringfile") % topic
            )
            return []
        host = next(self)
        return [topic + '.' + host]


class FanoutRingExchange(RingExchange):
    """Fanout Exchange"""
    def __init__(self):
        super(FanoutRingExchange, self).__init__()

    def run(self, context, topic):
        # Assume starts with "fanout."
        topic = topic.split('fanout.')[1:][0]
        return map(lambda x: (topic + '.' + x), self.ring[topic])


class MatchMakerRing(MatchMakerBase):
    """
    Match Maker where hosts are loaded from a static file
    """
    def __init__(self):
        super(MatchMakerRing, self).__init__()  # *args, **kwargs)

        # Direct-to-host
        self.add_binding(DirectTopicBinding(), RoundRobinRingExchange())

        # Standard RR
        self.add_binding(BareTopicBinding(), RoundrobinRingExchange())

        # fanout messaging
        self.add_binding(FanoutBinding(), FanoutRingExchange())


class LocalhostExchange(Exchange):
    """Exchange where all direct topics are local"""
    def __init__(self):
        super(Exchange, self).__init__()

    def run(self, context, topic):
        return [topic + '.' + 'locahost']


class MatchMakerLocalhost(MatchMakerBase):
    """
    Match Maker where all bare topics resolve to localhost.
    Useful for testing.
    """
    def __init__(self):
        super(MatchMakerLocalhost, self).__init__()
        #self.add_binding(Binding(), RoundRobinRingExchange())
        self.add_binding(BareTopicBinding(), RoundRobinRingExchange())
        self.add_binding(BareTopicBinding(), FanoutRingExchange())
        self.add_binding(FanoutBinding(), FanoutRingExchange())

#    def queues(self, style, context, topic):
#        x=super(MatchMakerLocalhost, self).queues(style, context, topic)
#        print "Queues: %s" % x
#        return x

    def queues(self, context, topic):
        if '.' not in topic:
            return [(context, topic + '.localhost', 'localhost')]
        return [(context, topic, 'localhost'), ]


class MatchMakerPassthrough(MatchMakerBase):
    """
    Match Maker where topics are untouched.
    Useful for testing, or for AMQP/brokered queues.
    """
    def __init__(self):
        super(MatchMakerLocalhost, self).__init__()

    def queues(self, context, topic):
        return [(context, topic, topic), ]
