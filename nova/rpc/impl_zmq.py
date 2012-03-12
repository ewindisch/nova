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

zmq_opts = [
    # ZeroMQ bind 'host' should be a wildcard (*),
    # an ethernet interface, or an IP.

    cfg.StrOpt('rpc_zmq_bind_address', default='*',
        help='ZeroMQ bind address'),

    # The IP of the machine acting as a central broker.
    cfg.StrOpt('rpc_zmq_broker_ip', default='127.0.0.1',
        help='Host providing brokerage service.'),

    # The module to use for matchmaking.
    cfg.StrOpt('rpc_zmq_matchmaker', default='MatchMakerBroker',
        help='Match maker module.'),

    # Matchmaker ring file
    cfg.StrOpt('rpc_zmq_matchmaker_ringfile',
        default='/etc/nova/zmq_matchmaker_ring',
        help='Host providing match making service.'),

    cfg.IntOpt('rpc_zmq_start_port', default=9500,
        help='zmq first port (will consume subsequent ~50-75 TCP ports)'),

    ]

FLAGS = flags.FLAGS
FLAGS.register_opts(zmq_opts)
ZMQ_CTX = zmq.Context(1)

matchmaker = None


class TopicManager(object):
    """ TopicManager helps us manage our topics """
    ROUTER_PUSH = 0  # Input for central broker RR queue  (send to this)
    ROUTER_PULL = 1  # Output for central broker RR queue (pull from this)
    ROUTER_PUB = 2  # Input for Router P/S
    ROUTER_SUB = 3  # Output for Router P/S
    # For Multicast pubsub
    PUBSUB = 4
    # Distributed cast
    PUSH = 5

    # Useful for debugging
    socket_names = ['ROUTER_PUSH', 'ROUTER_PULL', 'ROUTER_PUB',
        'ROUTER_SUB', 'PUBSUB', 'PUSH']

    _topics = None

    def __init__(self):
        pass

    @classmethod
    def topics(self):
        '''
        Get the names of all nova topics.
        '''

        # If memoized...
        if TopicManager._topics:
            return TopicManager._topics

        topics = {}

        # TODO(ewindisch): static is ugly:
        # consider putting all incoming requests into the zmq-receiver
        # instead?

        # topic_flags contains all flags and their modules.
        topic_flags = {
            'compute_topic': 'nova.flags',
            'console_topic': 'nova.flags',
            'scheduler_topic': 'nova.flags',
            'volume_topic': 'nova.flags',
            'network_topic': 'nova.flags',
            'vsa_topic': 'nova.flags',
            'cert_topic': 'nova.flags',
            'consoleauth_topic': 'nova.consoleauth'
        }
        # Static list of topics
        # 'test' - used by test suite
        # 'nested' - used by test sutie
        expected_topics = ['test', 'nested', 'zmq_replies']

        for topic, module in topic_flags.items():
            flags.DECLARE(topic, module)

        # Concat dynamic and static topic lists
        expected_topics.extend(map(lambda x: getattr(FLAGS, x),
                               topic_flags.keys()))

        for i, topic in enumerate(expected_topics):
            topics[topic] = i

        # memoize
        TopicManager._topics = topics
        return topics

    @classmethod
    def topic_from_port(self, port):
        """ Returns address for ip/topic """
        topics = self.topics()
        x = (port - FLAGS.rpc_zmq_start_port) % len(topics)
        for k, v in topics.iteritems():
            if v == x:
                return k

    @classmethod
    def socket_type_from_port(self, port):
        """ Returns address for ip/topic """
        topics = self.topics()
        return self.socket_names[(port - FLAGS.rpc_zmq_start_port) / \
            (topics[self.topic_from_port(port)] + len(topics)) - 1]

    @classmethod
    def port(self, topic, socket_type):
        """ Returns port for a given topic """
        tsplit = topic.split(".", 3)
        base_topic = tsplit[0]

        padding = 0
        if len(tsplit) == 3:
            padding = int(tsplit[2])

        port_offset = self.topics()[base_topic] + \
            len(self.topics()) * (socket_type + 1)
        return FLAGS.rpc_zmq_start_port + port_offset + padding

    @classmethod
    def addr(self, topic, socket_type):
        """ Returns connection address for topic """

        tsplit = topic.split(".", 1)
        base_topic = tsplit[0]
        if 1 in tsplit:
            host = tsplit[1]
        else:
            host = FLAGS.rpc_zmq_broker_ip

        port = self.port(topic, socket_type)
        return "tcp://%s:%s" % (host, port)

    @classmethod
    def listen_addr(self, topic, socket_type):
        """ Returns listening address for topic """
        port = self.port(topic, socket_type)
        return "tcp://%s:%s" % \
                (FLAGS.rpc_zmq_bind_address,
                 port)


class MatchMakerBase(object):
    """Match Maker Base Class"""
    def __init__(self):
        pass

    def get_workers(self, context, sock_type, topic):
        # Get a host on bare topics.
        # Not needed for ROUTER_PUB which is always brokered.
        if '.' not in topic and sock_type != TopicManager.ROUTER_PUSH \
                            and sock_type != TopicManager.ROUTER_PUB:
            (topic, sock_type) = self.get_addr_args(context, topic)
        return [TopicManager.addr(topic, sock_type)]


class MatchMakerTopicScheduler(MatchMakerBase):
    """Match Maker where a get_worker request is routed."""
    def __init__(self):
        pass

    def get_addr_args(self, context, topic):
        host = _multi_send("call", context,
            "%s" % (topic),
            {'method': '-get_worker', 'args': {}},
            timeout=5, sock_type=TopicManager.ROUTER_PUSH)[2][0]
        topic = topic + "." + host
        return (topic, TopicManager.PUSH)


class MatchMakerBroker(MatchMakerBase):
    """Match Maker where all bare topics are routed"""
    def __init__(self):
        pass

    def get_addr_args(self, context, topic):
        return (topic, TopicManager.ROUTER_PUSH)


class MatchMakerRing(MatchMakerBase):
    """Match Maker where hosts are loaded from a static file"""
    def __init__(self):
        fh = open(FLAGS.rpc_zmq_matchmaker_ringfile, 'r')
        self.ring = json.load(fh)
        self.ring0 = {}
        for k in self.ring.keys():
            self.ring0 = itertools.cycle(self.ring[k])
        fh.close()
        LOG.debug(_("RING:\n%s"), self.ring0)

    def get_addr_args(self, context, topic):
        host = next(self.ring0)
        return (topic + '.' + host, TopicManager.PUSH)


class MatchMakerFanoutRing(MatchMakerRing):
    """
       Match Maker where hosts are loaded from a static file
       - with support for brokerless fanout
    """
    def get_workers(self, context, sock_type, topic):
        # Get a host on bare topics.
        # Not needed for ROUTER_PUB which is always brokered.
        if '.' not in topic and sock_type != TopicManager.ROUTER_PUSH:
            (topic, sock_type) = self.get_addr_args(context, topic)
        elif sock_type == TopicManager.ROUTER_PUB:
            sock_type = TopicManager.PUSH
            return map(lambda h: TopicManager.addr(topic + '.' + h, sock_type),
                       self.ring0[topic])
        return [TopicManager.addr(topic, sock_type)]


class QueueSocket(object):
    """
    A tiny wrapper around ZeroMQ to simplify the send/recv protocol
    and connection management.
    """

    def __init__(self, addr, zmq_type, bind=True, recv=True, send=True,
                 subscribe=None):
        self.sock = ZMQ_CTX.socket(zmq_type)
        self.can_multi_send = send
        self.can_recv = recv
        self.subscribe = subscribe
        self.addr = addr

        if self.subscribe:
            self.sock.setsockopt(zmq.SUBSCRIBE, subscribe)

        if bind:
            self.sock.bind(addr)
        else:
            self.sock.connect(addr)

    def close(self):
        if self.subscribe:
            self.sock.setsockopt(zmq.UNSUBSCRIBE, self.subscribe)
        if not self.sock.closed:
            self.sock.close(linger=-1)
        self.sock = None

    def recv(self):
        assert self.can_recv, _("You cannot recv on this socket.")
        return self.sock.recv_multipart()

    def send(self, data):
        self.sock.send_multipart(data)


class ZmqClient(object):
    """Client for ZMQ sockets"""

    def __init__(self, addr, socket_type=zmq.PUSH, bind=False,
            recv=False):
        self.outq = QueueSocket(addr, socket_type, bind=bind, recv=recv)

    def cast(self, msg_id, topic, data):
        self.outq.send([str(msg_id), str(topic), str('cast'),
                        pickle.dumps(data, protocol=2)])

    def close(self):
        self.outq.close()


class RpcContext(context.RequestContext):
    """ Context that supports replying to a rpc.call """
    def __init__(self, *args, **kwargs):
        self.replies = []
        super(RpcContext, self).__init__(*args, **kwargs)

    def reply(self, reply=None, failure=None, ending=False):
        if ending:
            return
        self.replies.append(reply)

    @classmethod
    def marshal(self, ctx):
        ctx_data = ctx.to_dict()
        return pickle.dumps(ctx_data, protocol=2)

    @classmethod
    def unmarshal(self, data):
        return RpcContext.from_dict(pickle.loads(data))


class InternalContext(object):
    """Used by ConsumerBase as a private context for - methods"""

    def __init__(self, proxy):
        self.proxy = proxy
        self.msg_waiter = None

    def connect(self):
        if not self.msg_waiter:
            self.msg_waiter = ZmqClient('inproc://zmq_reply_queue')

    def get_worker(self, ctx):
        return socket.gethostname()

    def process_reply(self, ctx, msg_id=None, response=None):
        """Process a reply"""
        self.connect()
        self.msg_waiter.cast(str(msg_id), str('zmq_replies'), response)

    def get_response(self, ctx, proxy, topic, data):
        """Process a curried message and cast the result to topic"""
        # Internal method
        # uses internal ctx for safety.
        if data['method'][0] == '-':
            # For reply / process_reply
            method = method[1:]
            proxy = self
            if method == 'get_worker':
                return ConsumerBase.normalize_reply(
                    self.get_worker(ctx, **data['args']),
                    ctx.replies
                )
            return

        func = getattr(proxy, data['method'])

        try:
            if 'args' in data:
                result = func(ctx, **data['args'])
            else:
            	result = func(ctx)
            return ConsumerBase.normalize_reply(result, ctx.replies)
        except GreenletExit:
            # ignore these since they are just from shutdowns
            pass
        except Exception:
            return ConsumerBase.build_exception(sys.exc_info())

    def reply(self, ctx, proxy,
              msg_id=None, context=None, topic=None, msg=None):
        """Reply to a casted call"""
        # Our real method is curried into msg['args']

        child_ctx = RpcContext.unmarshal(msg[0])
        response = ConsumerBase.normalize_reply(
            self.get_response(child_ctx, proxy, topic, msg[1]),
            ctx.replies)

        _multi_send("cast", ctx, topic, {
            'method': '-process_reply',
            'args': {
                'msg_id': msg_id,
                'response': response
            }
        })


class ConsumerBase(object):
    """ Base Consumer """

    def __init__(self):
        self.private_ctx = InternalContext(None)

    @classmethod
    def normalize_reply(self, result, replies):
        if isinstance(result, types.GeneratorType):
            return list(result)
        elif replies:
            return replies
        else:
            return [result]

    @classmethod
    def build_exception(self, failure):
        """
        A list is always returned, but an exception is
        a dict so that the caller can differentiate exception
        responses from data responses.
        """
        tb = traceback.format_exception(*failure)
        failure = {'exc': (failure[0].__name__,
                            str(failure[1]), tb)}
        return failure

    def process(self, style, target, proxy, ctx, data):
        # Method starting with - are
        # processed internally. (non-valid method name)
        #proxy = self.proxy
        method = data['method']

        # Internal method
        # uses internal context for safety.
        if data['method'][0] == '-':
            # For reply / process_reply
            method = method[1:]
            iproxy = self.private_ctx  # self
            if method == 'reply':
                self.private_ctx.reply(ctx, proxy, **data['args'])
                return None
            elif method == 'process_reply':
                return self.private_ctx.process_reply(ctx, **data['args'])
            return
        else:
            iproxy = proxy

        try:
            func = getattr(iproxy, data['method'])
        except AttributeError:
            return ConsumerBase.build_exception(sys.exc_info())

        func(ctx, **data['args'])
        return None


class ZmqReactor(ConsumerBase):
    """
     A consumer class implementing a
     centralized casting broker (PULL-PUSH)
     for RoundRobin requests.
    """

    def __init__(self):
        super(ZmqReactor, self).__init__()

        self.mapping = {}
        self.proxies = {}
        self.threads = []
        self.sockets = []

    def register(self, proxy, in_addr, zmq_type_in, out_addr=None,
                 zmq_type_out=None, in_bind=True, out_bind=True,
                 subscribe=None):

        LOG.debug(_("Registering reactor"))

        # Items push in.
        inq = QueueSocket(in_addr, zmq_type_in, bind=in_bind,
                          subscribe=subscribe)

        self.proxies[inq] = proxy
        self.sockets.append(inq)

        LOG.debug(_("In reactor registered"))

        if not out_addr:
            return None

        # Items push out.
        outq = QueueSocket(out_addr, zmq_type_out,
                           bind=out_bind)

        self.mapping[inq] = outq
        self.mapping[outq] = inq
        self.sockets.append(outq)

        LOG.debug(_("Out reactor registered"))

    def _procsocket(self, sock):
        while True:
            data = sock.recv()
            if sock in self.mapping:
                #LOG.debug(_("ROUTER RELAY-OUT %(data)s") % {
                #    'data': data})
                self.mapping[sock].send(data)
            else:
                #LOG.debug(_("CONSUMER GOT %s") % \
                #            ' '.join(map(pformat, data)))

                msg_id, topic, style, in_msg = data

                #LOG.debug(_("DATA: %s") % \
                #            ' '.join(map(pformat, in_msg)))
                ctx, request = pickle.loads(in_msg)
                ctx = RpcContext.unmarshal(ctx)

                proxy = self.proxies[sock]

                eventlet.spawn_n(self.process, style, topic,
                                 proxy, ctx, request)

    def consume(self):
        for k in self.proxies.keys():
            self.threads.append(
                eventlet.spawn(self._procsocket, k)
            )

    def close(self):
        for s in self.sockets:
            s.close()

        for t in self.threads:
            t.kill()


class Connection(object):
    """ Manages connections and threads. """

    def __init__(self, isbroker=False):
        self.reactor = ZmqReactor()

    def create_consumer(self, topic, proxy, fanout, isbroker=False,
                        replysvc=False):
        if replysvc:
            ipc_dir = '/var/run/nova'

            in_addr = 'inproc://zmq_reply_queue'
            out_addr = "ipc://%s/zmq_reply_queue" % ipc_dir

            if not os.path.isdir(ipc_dir):
                utils.execute('mkdir', '-p', ipc_dir, run_as_root=True)
                utils.execute('chown', "%s:%s" % (os.getuid(), os.getgid()),
                              ipc_dir, run_as_root=True)
                utils.execute('chmod', '750', ipc_dir, run_as_root=True)

            self.reactor.register(proxy,
                                  in_addr, zmq.PULL, out_addr, zmq.PUB)
        elif isbroker and fanout:
            LOG.debug(_("Create Consumer FO-Router for (%(topic)s)") %
                {'topic': topic})
            in_addr = TopicManager.listen_addr(topic, TopicManager.ROUTER_SUB)
            out_addr = TopicManager.listen_addr(topic, TopicManager.ROUTER_PUB)

            self.reactor.register(
                proxy, in_addr, zmq.PULL, out_addr, zmq.PUB)
        elif isbroker:
            LOG.debug(_("Create Consumer PUSH/PULL-Pair for (%(topic)s)") %
                {'topic': topic})
            in_addr = TopicManager.listen_addr(topic,
                TopicManager.ROUTER_PUSH)
            out_addr = TopicManager.listen_addr(topic,
                TopicManager.ROUTER_PULL)

            self.reactor.register(proxy,
                                  in_addr, zmq.PULL, out_addr,
                                  zmq.PUSH)
        elif fanout:
            LOG.debug(_("Create Consumer FO for topic (%(topic)s)") %
                {'topic': topic})

            # PubSub
            in_addr = TopicManager.addr(topic, TopicManager.ROUTER_SUB)
            self.reactor.register(proxy, in_addr, zmq.SUB, subscribe='',
                                  in_bind=False)
        elif '.' in topic:
            LOG.debug(_("Create Consumer RR for topic (%(topic)s)") %
                {'topic': topic})

            inaddr = TopicManager.listen_addr(topic, TopicManager.PUSH)
            self.reactor.register(proxy, inaddr, zmq.PULL)
        else:
            # Connect to broker.
            # Depending on the matchmaker, requests may not come in this
            # way, but we support it anyway.

            inaddr = TopicManager.addr(topic, TopicManager.ROUTER_PULL)
            self.reactor.register(proxy, inaddr, zmq.PULL, in_bind=False)

    def close(self):
        self.reactor.close()

    def wait(self):
        # Greenthread.wait() doesn't seem to
        # allow other threads to be scheduled.
        # Thus, we just sleep here.

        # TODO(ewindisch): actually wait on
        #                  threads.
        while True:
            eventlet.sleep(28800)

    def consume(self, limit=None):
        self.reactor.consume()

    def consume_in_thread(self):
        eventlet.spawn(self.consume)


def _send(addr, style, context, topic, msg, socket_type=None, timeout=None):
    timeout = timeout or FLAGS.rpc_response_timeout
    conn = ZmqClient(addr, socket_type)

    if style == 'cast':
        try:
            # Casts should be quick, don't use standard time-out.
            with Timeout(30, exception=nova.rpc.common.Timeout) as t:
                payload = [RpcContext.marshal(context), msg]
                # assumes cast can't return an exception
                return conn.cast(topic, topic, payload)
        except Timeout:  # Ignore the timeouts
            pass
        finally:
            conn.close()
            return
    elif style != 'call':
        assert False, _("Invalid call style: %s") % style
        return

    # if style == call:
    LOG.debug("CALL CALL SAID THE RAVEN")

    # The msg_id is used to track replies.
    msg_id = str(uuid.uuid4().hex)
    hostname = socket.gethostname()
    base_topic = topic.split('.', 1)[0]

    # Replies always come into the reply service.
    reply_topic = "zmq_replies.%s" % hostname

    # Curry the original request into a reply method.
    orig_payload = [RpcContext.marshal(context), msg]
    payload = [RpcContext.marshal(context), {
        'method': '-reply',
        'args': {
            'msg_id': msg_id,
            'context': RpcContext.marshal(context),
            'topic': reply_topic,
            'msg': orig_payload
        }
    }]

    # Messages arriving async.
    msg_waiter = QueueSocket(
        "ipc:///var/run/nova/zmq_reply_queue",
        zmq.SUB, subscribe=msg_id, bind=False)

    try:
        with Timeout(timeout, exception=nova.rpc.common.Timeout) as t:
            # We timeout no more than 30 seconds for the cast itself.
            with Timeout(30, exception=nova.rpc.common.Timeout) as t1:
                conn.cast(msg_id, topic, payload)

            # Blocks until receives reply
            responses = pickle.loads(msg_waiter.recv()[-1])
    finally:
        conn.close()
        msg_waiter.close()
        del msg_waiter

    # It seems we don't need to do all of the following,
    # but perhaps it would be useful for multicall?
    # One effect of this is that we're checking all
    # responses for Exceptions.
    all_data = []
    for resp in responses:
        if isinstance(resp, types.DictType) and 'exc' in resp:
            raise RemoteError(*resp['exc'])
        all_data.append(resp)

    return style, topic, all_data[-1]


def _multi_send(style, context, topic, msg, socket_type=None, timeout=None):
    """
    Implements sending of messages.
    Determines which address to send a message to
    and sends a message, manages replies from call()
    """

    # We memoize matchmaker through this global
    global matchmaker

    socket_type = socket_type or TopicManager.PUSH

    if topic.endswith(".None") or topic.endswith("."):
        topic = topic.split(".")[0]

    if not matchmaker:
        constructor = globals()[FLAGS.rpc_zmq_matchmaker]
        matchmaker = constructor()
    addresses = matchmaker.get_workers(context, socket_type, topic)

    # This supports brokerless fanout (addresses > 1)
    for addr in addresses:
        if style == "cast":
            eventlet.spawn_n(_send, addr, style, context, topic, msg,
                             socket_type, timeout)
        else:
            return _send(addr, style, context, topic,
                         msg, socket_type, timeout)


def create_connection(new=True):
    return Connection()


def multicall(context, topic, msg, timeout=None):
    """ Multiple calls """
    LOG.debug(_("RR MULTICALL %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    style, target, data = _multi_send("call", context, str(topic), msg,
        timeout=timeout)
    return data


def call(context, topic, msg, timeout=None):
    """ Send a message, expect a response """
    LOG.debug(_("RR CALL %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    style, target, data = _multi_send("call", context, str(topic), msg,
        timeout=timeout)
    return data[-1]


def cast(context, topic, msg):
    """ Send a message expecting no reply """
    LOG.debug(_("RR CAST %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    _multi_send("cast", context, str(topic), msg)


def fanout_cast(context, topic, msg):
    """ Send a message to all listening and expect no reply """
    LOG.debug(_("FANOUT CAST %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    _multi_send("cast", context, str(topic), msg,
          socket_type=TopicManager.ROUTER_PUB)


def notify(context, topic, msg):
    """Send notification event."""
    cast(context, topic, msg)


def cleanup():
    """Clean up resoruces in use by implementation."""

    # NOTE(ewindisch): All cleanup should be handled by
    # Connection.close().  Managing the connections
    # through a class variable/method would be ugly & broken.
    pass
