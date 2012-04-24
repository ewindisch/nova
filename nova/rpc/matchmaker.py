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


zmq_opts = [
    # Matchmaker ring file
    cfg.StrOpt('rpc_zmq_matchmaker_ringfile',
        default='/etc/nova/matchmaker_ring.json',
        help='Matchmaker ring file (JSON)'),
]

FLAGS = flags.FLAGS
FLAGS.register_opts(zmq_opts)


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

    def add_condition(self, condition, rule):
        self.conditions.append((condition, rule, False))

    def add_negate_condition(self, condition, rule):
        self.conditions.append((condition, rule, True))

    def get_workers(self, context, topic):
        workers = []
        for (condition, rule, bit) in self.conditions:
        	x = condition.run(context, topic)
        	if (bit and not x) or x:
        		workers.append(rule.run(context, topic))
        return workers


# Get a host on bare topics.
# Not needed for ROUTER_PUB which is always brokered.
class RulePass(RewriteRule):
    def run(self, context, topic):
        return (context, topic)


# Get a host on bare topics.
# Not needed for ROUTER_PUB which is always brokered.
class ConditionBareTopic(RewriteCond):
    def _test(self, context, topic):
        if '.' not in topic:
            return True
        return False


class MatchMakerRing(MatchMakerBase):
    """
        Match Maker where hosts are loaded from a static file
    """
    def __init__(self):
        fh = open(FLAGS.rpc_zmq_matchmaker_ringfile, 'r')
        self.ring = json.load(fh)
        self.ring0 = {}
        for k in self.ring.keys():
            self.ring0[k] = itertools.cycle(self.ring[k])
        fh.close()
        LOG.debug(_("RING:\n%s"), self.ring0)

        for key in TopicManager.topics().keys():
            if not key in self.ring:
                LOG.warning(_("MatchMaker ringfile does not define topic:"
                            "%s") % key)

        # round-robin
        self.add_condition(ConditionBareTopic(), NextTopicRule())
        # fanout messaging
        self.add_condition(ConditionBareTopic(), AllTopicRule())

    def NextTopicRule(RewriteRule):
        def run(self, context, topic):
            if topic not in self.ring0:
                LOG.debug(
                    _("No key defining hosts for topic '%(topic)', "
                      "see ringfile") % topic
                )
                return []
            host = next(self.ring0[topic])
            return (topic + '.' + host, TopicManager.PUSH)
        
    class AllTopicRule(RewriteRule):
        def run(self, context, topic):
            return ring0[topic]
