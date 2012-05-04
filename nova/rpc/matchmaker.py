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
import cPickle as pickle
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

from eventlet.timeout import Timeout

from nova import context
from nova.openstack.common import cfg
import nova.rpc.common
from nova.rpc.common import RemoteError, LOG
from nova import flags
from nova import utils


contextmanager = contextlib.contextmanager

zmq_opts = [
    # Matchmaker ring file
    cfg.StrOpt('rpc_zmq_matchmaker_ringfile',
        default='/etc/nova/matchmaker_ring.json',
        help='Matchmaker ring file (JSON)'),
]

FLAGS = flags.FLAGS
FLAGS.register_opts(zmq_opts)


class MatchMakerException(exception.NovaException):
    """Signified a match could not be found."""
    message = _("Match not found by MatchMaker.")


class RewriteRule(object):
    """
    Implements lookups.
    Subclass this to support hashtables, dns, etc.
    """
    def __init__(self):
        pass

    def run(self, context, topic):
        raise NotImplementedError()


class RewriteCond(object):
    """
    A condition on which to perform a lookup.
    """
    def __init__(self):
        pass

    def _test(self, context, topic):
        raise NotImplementedError()

    def run(self, context, topic):
        if self._test(context, topic):
            return True
        return False


class MatchMakerBase(object):
    """Match Maker Base Class"""

    def __init__(self):
        # Array of tuples. Index [2] toggles negation
        self.conditions = []

    def add_condition(self, condition, rule, last=False):
        self.conditions.append((condition, rule, False, last))

    def add_negate_condition(self, condition, rule, last=False):
        self.conditions.append((condition, rule, True, last))

    def get_workers(self, style, context, topic):
        workers = []
        for (condition, rule, bit, last) in self.conditions:
            #x = condition.run(context, topic)
            #if (bit and not x) or x:
            #   workers.extend(rule.run(style, context, topic))
            with condition:
                workers.extend(rule.run(style, context, topic))

            if len(workers) >= limit:
                return workers[0:limit]
        return workers


# Get a host on bare topics.
# Not needed for ROUTER_PUB which is always brokered.
class RulePass(RewriteRule):
    def run(self, style, context, topic):
        return (style, context, topic)


class PrettyContext(object):
    def __init__(self, method):
        self.meth = method
        #pass

    def __enter__(self):
        try:
            return self.meth.next()
        except StopIteration:
            return
        #return self

    def __exit__(self, type, value, tb):
        try:
            return self.meth.next()
        except StopIteration:
            return True


def prettycontext(fun):
    def helper(*args, **kwargs):
        return(PrettyContext(fun(*args, **kwargs)))
    return helper


## Get a host on bare topics.
## Not needed for ROUTER_PUB which is always brokered.
#@prettycontext  #contextmanager
#def condbaretopic(style, context, topic):
#    if '.' not in topic:
#    	print "foo!"
#        yield
#
#with condbaretopic('a.a','b.d','cd'):
#    print "hello world"
#
#with condbaretopic('a.a','b.d','c.d'):
#    print "hello world"


@prettycontext
def condfanout(style, context, topic):
    if topic.startswith('fanout'):
        yield


class ConditionBareTopic(RewriteCond):
    #def _test(
    def __enter__(self, style, context, topic):
        if '.' not in topic:
            return True
        return False


# Get a host on bare topics.
# Not needed for ROUTER_PUB which is always brokered.
class ConditionFanout(RewriteCond):
    def __enter__(self, style, context, topic):
        if topic.startswith('fanout'):
            return True
        return False


class RingRule(RewriteRule):
    """
    Match Maker where hosts are loaded from a static file
    """
    def __init__(self):
        super(RingRule, self).__init__()

        fh = open(FLAGS.rpc_zmq_matchmaker_ringfile, 'r')
        self.ring = json.load(fh)
        self.ring0 = {}
        for k in self.ring.keys():
            self.ring0[k] = itertools.cycle(self.ring[k])
        fh.close()
        LOG.debug(_("RING:\n%s"), self.ring0)


class NextTopicRule(RingRule):
    def __init__(self):
        super(NextTopicRule, self).__init__()

    def run(self, context, topic):
        if topic not in self.ring0:
            LOG.debug(
                _("No key defining hosts for topic '%(topic)', "
                  "see ringfile") % topic
            )
            return []
        host = next(self.ring0[topic])
        return [topic + '.' + host]


class AllTopicRule(RingRule):
    def __init__(self):
        super(AllTopicRule, self).__init__()

    def run(self, context, topic):
        return map(lambda x: (topic + '.' + x), self.ring[topic])


class MatchMakerRing(MatchMakerBase):
    """
        Match Maker where hosts are loaded from a static file
    """
    def __init__(self):
        super(MatchMakerRing, self).__init__()  # *args, **kwargs)

        # round-robin
        ##self.add_condition(ConditionBareTopic(), NextTopicRule(), last=True)
        #self.add_condition(condbaretopic, NextTopicRule(), last=True)

        # fanout messaging
        self.add_condition(
            [ConditionBareTopic(), ConditionFanout()],
            AllTopicRule()
        )

        fh = open(FLAGS.rpc_zmq_matchmaker_ringfile, 'r')
        self.ring = json.load(fh)
        self.ring0 = {}
        for k in self.ring.keys():
            self.ring0[k] = itertools.cycle(self.ring[k])
        fh.close()
        LOG.debug(_("RING:\n%s"), self.ring0)

    def get_workers(self, style, context, topic):
        #raise Exception, "Set Sail for Fail"
        if topic not in self.ring:
            return []
        if style.startswith("fanout"):
            return self.ring[topic]
        if '.' not in topic:
            return self.ring0[topic].next()
        return [(style, context, topic), ]


class MatchMakerLocalhost(MatchMakerBase):
    """
    Match Maker where all bare topics resolve to localhost.
    Useful for testing.
    """
    def __init__(self):
        super(MatchMakerLocalhost, self).__init__()

    def get_workers(self, style, context, topic):
        if '.' not in topic:
            return [(style, context, topic + '.localhost', 'localhost')]
        return [(style, context, topic, 'localhost'), ]


class MatchMakerPassthrough(MatchMakerBase):
    """
    Match Maker where topics are untouched.
    Useful for testing, or for AMQP/brokered queues.
    """
    def __init__(self):
        super(MatchMakerLocalhost, self).__init__()

    def get_workers(self, style, context, topic):
        return [(style, context, topic, topic), ]
