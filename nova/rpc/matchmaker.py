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

from eventlet.green import zmq
from eventlet.timeout import Timeout

from nova import context
from nova.openstack.common import cfg
import nova.rpc.common
from nova.rpc.common import RemoteError, LOG
from nova import flags
from nova import utils


FLAGS = flags.FLAGS
flags.DECLARE('nova.rpc.impl_zmq', 'rpc_zmq_matchmaker')
flags.DECLARE('nova.rpc.impl_zmq', 'rpc_zmq_matchmaker_ringfile')


class MatchMakerBase(object):
    """Match Maker Base Class"""

    class RewriteRule(object):
        def __init__(self):
            pass

        def eval(self, context, topic):
            raise NotImplementedError()

    class RewriteCond(object):
        def __init__(self, rule):
            self.rule = rule
            pass

        def _test(self, context, sock_type, topic):
            raise NotImplementedError()

        def eval(self):
            if test():
                self.rule.eval()

    # Get a host on bare topics.
    # Not needed for ROUTER_PUB which is always brokered.
    class ConditionBareTopic(RewriteCond):
        def _test(self, context, sock_type, topic):
            if '.' not in topic and sock_type != TopicManager.ROUTER_PUSH \
                                and sock_type != TopicManager.ROUTER_PUB:
                return True
            return False

    def __init__(self):
        self.conditions = []

    def _add_condition(self, condition):
        self.conditions.append(condition)

    def _rewrite_topic(self, context, topic):
        """Rewrites the topic"""
        raise NotImplementedError()

    def get_workers(self, context, sock_type, topic):
        for condition in self.conditions:
        	condition.exec(context, sock_type, topic)


class MatchMakerTopicScheduler(MatchMakerBase):
    """
       Match Maker where a get_worker request is routed/brokered.
       This effectively acts as a brokered scheduler to
       facilitate peer2peer communicatons.
    """
    def __init__(self):
        pass

    def rewrite_topic(self, context, topic):
        host = _multi_send("call", context,
            "%s" % (topic),
            {'method': '-get_worker', 'args': {}},
            timeout=5, sock_type=TopicManager.ROUTER_PUSH)[2][0]
        topic = topic + "." + host
        return (topic, TopicManager.PUSH)


class MatchMakerBroker(MatchMakerBase):
    """Match Maker where all bare topics are routed/brokered"""
    def __init__(self):
        pass

    def rewrite_topic(self, context, topic):
        return (topic, TopicManager.ROUTER_PUSH)


class MatchMakerRing(MatchMakerBase):
    """
        Match Maker where hosts are loaded from a static file
        Fanout messages are brokered, all others are peer-to-peer.
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

    def rewrite_topic(self, context, topic):
        if topic not in self.ring0:
            LOG.debug(
                _("No key defining hosts for topic '%(topic)', "
                  "see ringfile") % topic
            )
            return []
        host = next(self.ring0[topic])
        return (topic + '.' + host, TopicManager.PUSH)


class MatchMakerFanoutRing(MatchMakerRing):
    """
       Match Maker where hosts are loaded from a static file
       All messages are peer2peer, including fanout.
    """
    def rewrite_cond(self, context, sock_type, topic):
        # Get a host on bare topics.
        # Not needed for ROUTER_PUB which is always brokered.
        if '.' not in topic and sock_type != TopicManager.ROUTER_PUSH:
            (topic, sock_type) = self.rewrite_topic(context, topic)
        elif sock_type == TopicManager.ROUTER_PUB:
            sock_type = TopicManager.PUSH
            return map(lambda h: TopicManager.addr(topic + '.' + h, sock_type),
                       self.ring0[topic])
        return [TopicManager.addr(topic, sock_type)]
