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

    def test(self, topic):
        raise NotImplementedError()


class MatchMakerBase(object):
    """Match Maker Base Class"""

    def __init__(self):
        # Array of tuples. Index [2] toggles negation, [3] is last-if-true
        self.bindings = []

    def add_binding(self, binding, rule, last=True):
        self.bindings.append((binding, rule, False, last))

    #def add_negate_binding(self, binding, rule, last=True):
    #    self.bindings.append((binding, rule, True, last))

    def queues(self, key):
        workers = []

        # bit is for negate bindings - if we choose to implement it.
        # last stops processing rules if this matches.
        for (binding, exchange, bit, last) in self.bindings:
            if binding.test(key):
                workers.extend(exchange.run(context, key))

                # Support last.
                if last:
                	return workers
        return workers


class DirectBinding(Binding):
    """
        Specifies a host in the key via a '.' character
        Although dots are used in the key, the behavior here is
        that it maps directly to a host, thus direct.
    """
    def test(self, key):
        if '.' in key:
            return True
        return False


class TopicBinding(Binding):
    """
       Where a 'bare' key without dots.
       AMQP generally considers topic exchanges to be those *with* dots,
       but we deviate here in terminology as the behavior here matches
       that of a topic exchange (whereas where there are dots, behavior
       matches that of a direct exchange.
    """
    def test(self, key):
        if '.' not in key:
            return True
        return False


class FanoutBinding(Binding):
    """Match on fanout keys, where key starts with 'fanout.' string."""
    def test(self, topic):
        if topic.startswith('fanout.'):
            return True
        return False


class StubExchange(Exchange):
    """Exchange that does nothing"""
    def run(self, topic):
        return [(topic, None)]


class RingExchange(Exchange):
    """
    Match Maker where hosts are loaded from a static file containing
    a hashmap (JSON formatted)
    """
    def __init__(self):
        super(RingExchange, self).__init__()

        fh = open(FLAGS.matchmaker_ringfile, 'r')
        self.ring = json.load(fh)
        self.ring0 = {}
        for k in self.ring.keys():
            self.ring0[k] = itertools.cycle(self.ring[k])
        fh.close()

    def next(self):
        return next(self.ring0[topic])

    def _ring_has(self, topic):
        if topic in self.ring0:
        	return True
        return False


class RoundRobinRingExchange(RingExchange):
    """A Topic Exchange based on a hashmap"""
    def __init__(self):
        super(RoundRobinRingExchange, self).__init__()

    def run(self, context, topic):
        if not self._ring_has(topic):
            LOG.warn(
                _("No key defining hosts for topic '%(topic)', "
                  "see ringfile") % topic
            )
            return []
        host = next(self)
        return [(topic + '.' + host, host)]


class FanoutRingExchange(RingExchange):
    """Fanout Exchange based on a hashmap"""
    def __init__(self):
        super(FanoutRingExchange, self).__init__()

    def run(self, context, topic):
        if not self._ring_has(topic):
            LOG.warn(
                _("No key defining hosts for topic '%(topic)', "
                  "see ringfile") % topic
            )
            return []
        # Assume starts with "fanout.", strip it.
        topic = topic.split('fanout.')[1:][0]
        return map(lambda x: (topic + '.' + x, x), self.ring[topic])


class LocalhostExchange(Exchange):
    """Exchange where all direct topics are local"""
    def __init__(self):
        super(Exchange, self).__init__()

    def run(self, context, topic):
        return [(topic + '.' + 'locahost', 'localhost')]


class MatchMakerRing(MatchMakerBase):
    """
    Match Maker where hosts are loaded from a static hashmap.
    """
    def __init__(self):
        super(MatchMakerRing, self).__init__()  # *args, **kwargs)

        # fanout messaging
        self.add_binding(FanoutBinding(), FanoutRingExchange())

        # Direct-to-host
        self.add_binding(DirectBinding(), RoundRobinRingExchange())

        # Standard RR
        self.add_binding(TopicBinding(), RoundRobinRingExchange())


class MatchMakerLocalhost(MatchMakerBase):
    """
    Match Maker where all bare topics resolve to localhost.
    Useful for testing.
    """
    def __init__(self):
        super(MatchMakerLocalhost, self).__init__()
        self.add_binding(FanoutBinding(), LocalhostExchange())
        self.add_binding(DirectBinding(), LocalhostExchange())
        self.add_binding(TopicBinding(), LocalhostExchange())


class MatchMakerStub(MatchMakerBase):
    """
    Match Maker where topics are untouched.
    Useful for testing, or for AMQP/brokered queues.
    """
    def __init__(self):
        super(MatchMakerLocalhost, self).__init__()

        self.add_binding(FanoutBinding(), StubExchange())
        self.add_binding(DirectBinding(), StubExchange())
        self.add_binding(TopicBinding(), StubExchange())
