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

# Eventlet/monkey_patch is an exception to HACKING
import eventlet
import greenlet
eventlet.monkey_patch()

import collections
import cPickle as pickle
import hashlib
import itertools
import json
import logging
import os
import pprint
import random
import string
import sys
import types
import traceback
import uuid

# These eventlets can be safely loaded later
from eventlet import greenpool
from eventlet.green import zmq
from eventlet import timeout as eventlet_timeout

import nova
from nova import context
from nova import flags
from nova import utils
from nova.openstack.common import cfg
from nova.rpc import common as rpc_common
from nova.rpc import matchmaker as mod_matchmaker

pformat = pprint.pformat
Timeout = eventlet_timeout.Timeout
#LOG = rpc_common.LOG
LOG = logging.getLogger(__name__)
RemoteError = rpc_common.RemoteError

zmq_opts = [
    # ZeroMQ bind 'host' should be a wildcard (*),
    # an ethernet interface, or an IP.

    cfg.StrOpt('rpc_zmq_bind_address', default='*',
        help='ZeroMQ bind address'),

    # The module to use for matchmaking.
    cfg.StrOpt('rpc_zmq_matchmaker', default='mod_matchmaker.MatchMakerRing',
        help='MatchMaker driver'),

    cfg.IntOpt('rpc_zmq_start_port', default=9500,
        help='zmq first port (will consume subsequent ~50-75 TCP ports)'),

    cfg.IntOpt('rpc_zmq_contexts', default=1,
        help='number of ZeroMQ contexts, defaults to 1'),

    cfg.StrOpt('rpc_zmq_ipc_dir', default='/var/run/nova',
        help='directory for holding IPC sockets')
    ]


ZMQ_CTX = None
matchmaker = None


def _serialize(data):
    """
    Serialization wrapper
    We prefer using JSON, but it cannot encode all types.
    If encoding fails as JSON, fallback to Pickle.
    """
    #TODO(ewindisch): verify if we can eliminate this and ONLY use JSON
    try:
        return json.dumps(data)
    except TypeError:
        LOG.warn(_("JSON serialization failed."
                   "Falling back to Pickle."))
        return pickle.dumps(data, version=2)


def _deserialize(data):
    """
    Deserialization wrapper
    We prefer using JSON, but cannot encode all types.
    If input is not JSON, fallback to Pickle.
    """
    #TODO(ewindisch): verify if we can eliminate this and ONLY use JSON
    try:
        return json.loads(data)
    except ValueError:
        return pickle.loads(data)


class ZmqSocket(object):
    """
    A tiny wrapper around ZeroMQ to simplify the send/recv protocol
    and connection management.

    Can be used as a Context (supports the 'with' statement).
    """

    def __init__(self, addr, zmq_type, bind=True, subscribe=None):
        self.sock = ZMQ_CTX.socket(zmq_type)
        self.addr = addr
        self.type = zmq_type
        self.subscriptions = []

        # Support failures on sending/receiving on wrong socket type.
        self.can_recv = zmq_type in (zmq.PULL, zmq.SUB)
        self.can_send = zmq_type in (zmq.PUSH, zmq.PUB)

        do_sub = {
        	list: subscribe,
        	str:  [subscribe],
        	type(None): []
        }[type(subscribe)]

        for f in do_sub:
            self.subscribe(f)

        LOG.debug(_("Connecting to %s with %s"), addr, self.socket_s())
        print _("Connecting to %s with %s"), addr, self.socket_s()

        if bind:
            self.sock.bind(addr)
        else:
            self.sock.connect(addr)

    def socket_s(self):
        """Get socket type as string"""
        t_enum=('PUSH', 'PULL', 'PUB', 'SUB', 'REP', 'REQ', 'ROUTER',
                'DEALER')
        return dict(map(lambda t: (getattr(zmq, t), t), t_enum))[self.type]

    def subscribe(self, msg_filter):
        #assert None, "subscribing msg_filter=%s" % msg_filter
        self.sock.setsockopt(zmq.SUBSCRIBE, msg_filter)
        self.subscriptions.append(msg_filter)
        #assert None, "subscribing msg_filter=%s" % msg_filter

    def close(self):
        # We must unsubscribe, or we'll leak descriptors.
        if len(self.subscriptions) > 0:
            for f in self.subscriptions:
                self.sock.setsockopt(zmq.UNSUBSCRIBE, f)

        # Linger -1 prevents lost/dropped messages
        if not self.sock.closed:
            self.sock.close(linger=-1)

        self.sock = None

    def recv(self):
        assert self.can_recv, _("You cannot recv on this socket.")
        return self.sock.recv_multipart()

    def send(self, data):
        assert self.can_send, _("You cannot send on this socket.")
        self.sock.send_multipart(data)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class ZmqClient(object):
    """Client for ZMQ sockets"""

    def __init__(self, addr, socket_type=zmq.PUSH, bind=False):
        self.outq = ZmqSocket(addr, socket_type, bind=bind)

    def cast(self, msg_id, topic, data):
        self.outq.send([str(msg_id), str(topic), str('cast'),
            _serialize(data)])

    def close(self):
        self.outq.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


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
        return _serialize(ctx_data)

    @classmethod
    def unmarshal(self, data):
        return RpcContext.from_dict(_deserialize(data))


class InternalContext(object):
    """Used by ConsumerBase as a private context for - methods"""

    def __init__(self, proxy):
        self.proxy = proxy
        self.msg_waiter = None

    def connect(self):
        if not self.msg_waiter:
            self.msg_waiter = ZmqClient('inproc://zmq_reply_queue')

    def process_reply(self, ctx, msg_id=None, response=None):
        """Process a reply"""
        print "Processing reply"
        self.connect()
        self.msg_waiter.cast(str(msg_id), str('zmq_replies'), response)

    def get_response(self, ctx, proxy, topic, data):
        """Process a curried message and cast the result to topic"""
#        # Internal method
#        # uses internal ctx for safety.
#        if data['method'][0] == '-':
#            # For reply / process_reply
#            method = method[1:]
#            proxy = self
#            return

        func = getattr(proxy, data['method'])

        try:
            if 'args' in data:
                result = func(ctx, **data['args'])
            else:
                result = func(ctx)
            return ConsumerBase.normalize_reply(result, ctx.replies)
        except greenlet.GreenletExit:
            # ignore these since they are just from shutdowns
            pass
        except Exception:
            return {'exc':
                    rpc_common.serialize_remote_exception(sys.exc_info())}

    def reply(self, ctx, proxy,
              msg_id=None, context=None, topic=None, msg=None):
        """Reply to a casted call"""
        # Our real method is curried into msg['args']

        child_ctx = RpcContext.unmarshal(msg[0])
        response = ConsumerBase.normalize_reply(
            self.get_response(child_ctx, proxy, topic, msg[1]),
            ctx.replies)

        print "Sending reply"
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
            return rpc_common.serialize_remote_exception(sys.exc_info())

        func(ctx, **data['args'])
        return None


class ZmqBaseReactor(ConsumerBase):
    """
     A consumer class implementing a
     centralized casting broker (PULL-PUSH)
     for RoundRobin requests.
    """

    def __init__(self, conf):
        super(ZmqBaseReactor, self).__init__()

        self.conf = conf
        self.mapping = {}
        self.proxies = {}
        self.topic_map = {}
        self.threads = []
        self.sockets = []
        self.sock_type = {}
        self.subscribe = {}
        self.publish = {}

        self.pool = greenpool.GreenPool(conf.rpc_thread_pool_size)

    def register(self, proxy, in_addr, zmq_type_in, out_addr=None,
                 zmq_type_out=None, in_bind=True, out_bind=True,
                 subscribe=None, publish=None):

        print "Registering reactor"
        LOG.debug(_("Registering reactor"))

        assert (zmq_type_in in (zmq.PULL, zmq.SUB)), "Bad input socktype"

        # Items push in.
        inq = ZmqSocket(in_addr, zmq_type_in, bind=in_bind,
                          subscribe=subscribe)

        self.proxies[inq] = proxy
        self.publish[inq] = publish
        self.sock_type[inq] = zmq_type_in
        self.sockets.append(inq)

        print "In reactor registered"
        LOG.debug(_("In reactor registered"))

        if not out_addr:
            print "No out addr"
            return

        assert (zmq_type_out in (zmq.PUSH, zmq.PUB)), "Bad output socktype"

        # Items push out.
        outq = ZmqSocket(out_addr, zmq_type_out,
                           bind=out_bind)

        self.mapping[inq] = outq
        self.mapping[outq] = inq
        self.sock_type[outq] = zmq_type_out
        self.sockets.append(outq)

        print "Out reactor registered"
        LOG.debug(_("Out reactor registered"))

    def consume_in_thread(self):
        def _consume(sock):
            print "Consuming socket"
            self.consume(sock)

        for k in self.proxies.keys():
            self.threads.append(
                self.pool.spawn(_consume, k)
            )

    def wait(self):
        for t in self.threads:
            t.wait()

    def close(self):
        for s in self.sockets:
            s.close()

        for t in self.threads:
            t.kill()


class ZmqProxy(ZmqBaseReactor):
    """
    A consumer class implementing a
    topic-based proxy, forwarding to
    IPC sockets.
    """

    def __init__(self, conf):
        self.topic_proxy = {}
        super(ZmqProxy, self).__init__(conf)

    def consume(self, sock):
        ipc_dir = self.conf.rpc_zmq_ipc_dir

        #TODO(ewindisch): use zero-copy (i.e. references, not copying)
        data = sock.recv()

        sock_type = self.sock_type[sock]

        LOG.debug(_("CONSUMER GOT %s") % \
                    ' '.join(map(pformat, data)))

        print _("CONSUMER GOT %s") % \
                    ' '.join(map(pformat, data))

        msg_id, topic, style, in_msg = data

        LOG.debug(_("DATA: %s") % \
                    ' '.join(map(pformat, in_msg)))

        print _("DATA: %s") % \
                    ' '.join(map(pformat, in_msg))
        ctx, request = _deserialize(in_msg)
        ctx = RpcContext.unmarshal(ctx)

        if not topic in self.topic_proxy:
            subscribe=(None, msg_id)[sock_type == zmq.SUB]
            outq = ZmqSocket("ipc://%s/zmq_topic_%s" % (ipc_dir, topic),
                               sock_type, subscribe=subscribe, bind=True)
            self.topic_proxy[topic] = outq
            self.sockets.append(outq)
            print "Relayed data to %s : %s" % (topic, subscribe)

        LOG.debug(_("ROUTER RELAY-OUT %(data)s") % {
            'data': data})
        self.topic_proxy[topic].send(data)


class ZmqReactor(ZmqBaseReactor):
    """
    A consumer class implementing a
    consumer for messages. Can also be
    used as a 1:1 proxy
    """

    def __init__(self, conf):
        super(ZmqReactor, self).__init__(conf)

    def consume(self, sock):
        #TODO(ewindisch): use zero-copy (i.e. references, not copying)
        print "CONSUMER GOT SOCKET: #1"
        data = sock.recv()
        print "CONSUMEr RECEIVED DATA: %s" % data
        if sock in self.mapping:
            LOG.debug(_("ROUTER RELAY-OUT %(data)s") % {
                'data': data})
            print _("ROUTER RELAY-OUT %(data)s") % {
                'data': data}
            self.mapping[sock].send(data)
            return

        LOG.debug(_("CONSUMER GOT %s") % \
                    ' '.join(map(pformat, data)))
        print _("CONSUMER GOT %s") % \
                    ' '.join(map(pformat, data))


        msg_id, topic, style, in_msg = data

        LOG.debug(_("DATA: %s") % \
                    ' '.join(map(pformat, in_msg)))
        ctx, request = _deserialize(in_msg)
        ctx = RpcContext.unmarshal(ctx)

        proxy = self.proxies[sock]

        self.pool.spawn_n(self.process, style, topic,
                          proxy, ctx, request)


class ZmqReplyReactor(ZmqBaseReactor):

    def __init__(self, conf):
        ipc_dir = conf.rpc_zmq_ipc_dir

        # Create the necessary directories/files for this service.
        if not os.path.isdir(ipc_dir):
            utils.execute('mkdir', '-p', ipc_dir, run_as_root=True)
            utils.execute('chown', "%s:%s" % (os.getuid(), os.getgid()),
                          ipc_dir, run_as_root=True)
            utils.execute('chmod', '750', ipc_dir, run_as_root=True)

        reactor = self  # impl_zmq.ZmqReactor()

        """
        OUTSIDE -> [consume_in] -> [consume_out][replies_in] -> [replies_out]
        """

        # Reply service
        replies_out = 'inproc://replies'
        replies_in = "ipc://%s/zmq_topic_zmq_replies" % ipc_dir
        reply_proxy = impl_zmq.InternalContext(None)

        # Subscribe to all topics,
        # forward over inproc.
        reactor.register(reply_proxy, replies_in, zmq.SUB,
                         replies_out, zmq.PUB, subscribe='')

        reactor.consume()
        reactor.wait()


class Connection(nova.rpc.common.Connection):
    """ Manages connections and threads. """

    def __init__(self, conf):
        self.conf = conf
        self.reactor = ZmqReactor(conf)

        print "Creating reply consumer"
        self.create_consumer('zmq_replies',
            InternalContext(None), '')

    def create_consumer(self, topic, proxy, fanout=False):
                        #subscribe=None, isbroker=False,
                        #replysvc=False):
        # Default, don't subscribe.
        subscribe = None
        sock_type=zmq.PULL

        LOG.debug(_("Create Consumer RR for topic (%(topic)s)") %
            {'topic': topic})

        print "Create Consumer RR for topic (%s)" % topic

        # Receive messages from (local) proxy
        inaddr = "ipc://%s/zmq_topic_%s" % \
            (self.conf.rpc_zmq_ipc_dir, topic)

        # Subscription scenarios
        if type(fanout) == str:
            subscribe = fanout
            sock_type=zmq.SUB
        elif fanout:
            subscribe = ''
            sock_type=zmq.SUB

        print "Consumer",
        print ['PULL', 'SUB'][sock_type==zmq.SUB]

        self.reactor.register(proxy, inaddr, sock_type,
                              subscribe=subscribe)

    def close(self):
        self.reactor.close()

    def wait(self):
        # Greenthread.wait() blocks all threads
        # which is not what we want, so
        # we just sleep here.

        # TODO(ewindisch): actually wait on
        #                  threads.
        while True:
            eventlet.sleep(28800)
        #self.reactor.wait()

    def consume_in_thread(self):
        self.reactor.consume_in_thread()


def _send(conf, addr, style, context, topic, msg, timeout=None):
    # timeout_response is how long we wait for a response
    timeout_response = timeout or conf.rpc_response_timeout
    # timeout_msg is for another host to receive the message
    timeout_cast = conf.rpc_cast_timeout

    if style == 'cast':
        with ZmqClient(addr) as conn, \
             Timeout(timeout_cast, exception=nova.rpc.common.Timeout) as t:

            payload = [RpcContext.marshal(context), msg]

            # assumes cast can't return an exception
            return conn.cast(topic, topic, payload)

    if style != 'call':
        assert False, _("Invalid call style: %s") % style

    # The msg_id is used to track replies.
    msg_id = str(uuid.uuid4().hex)
    hostname = conf.host
    base_topic = topic.split('.', 1)[0]

    # Replies always come into the reply service.
    reply_topic = "zmq_replies.%s" % hostname

    print "Creating payload"
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

    print "Creating queue socket for reply waiter"

    # Messages arriving async.
    with \
        ZmqSocket(
            "ipc://%s/zmq_topic_zmq_replies" % conf.rpc_zmq_ipc_dir,
            zmq.SUB, subscribe=msg_id, bind=False
        ) as msg_waiter, \
        Timeout(
            timeout_response,
            exception=nova.rpc.common.Timeout
        ) as t_call:

            with \
                 ZmqClient(addr) as conn, \
                 Timeout(
                    timeout_cast,
                    exception=nova.rpc.common.Timeout
                 ) as t_cast:

                print "Sending cast"
                conn.cast(msg_id, topic, payload)

            print "Cast sent; Waiting reply"
            # Blocks until receives reply
            responses = _deserialize(msg_waiter.recv()[-1])

    print "Unpacking response"

    # It seems we don't need to do all of the following,
    # but perhaps it would be useful for multicall?
    # One effect of this is that we're checking all
    # responses for Exceptions.
    all_data = []
    for resp in responses:
        if isinstance(resp, types.DictType) and 'exc' in resp:
            #raise RemoteError(*resp['exc'])
            raise rpc_common.deserialize_remote_exception(conf, resp['exc'])
        all_data.append(resp)

    return style, topic, all_data[-1]


def _multi_send(conf, style, context, topic, msg,
     socket_type=None, timeout=None):
    """
    Wraps the sending of messages,
    dispatches to the matchmaker and sends
    message to all relevant hosts.
    """
    queues = matchmaker.queues(style, context, topic)

    LOG.debug(_("Sending message(s) to: %s") % queues)

    # Don't stack if we have no matchmaker results
    if len(queues) == 0:
        LOG.debug(_("No matchmaker results. Not casting."))
        # While not strictly a timeout, callers know how to handle
        # this exception and a timeout isn't too big a lie.
        raise nova.rpc.common.Timeout, "No match from matchmaker."

    # This supports brokerless fanout (addresses > 1)
    for queue in queues:
        (_style, _context, _topic, ip_addr) = queue
        _addr = "tcp://%s:%s" % (ip_addr, conf.rpc_zmq_start_port)

        if style.endswith("cast"):
            eventlet.spawn_n(_send, conf, _addr, _style, _context,
                             _topic, msg, timeout)
            return
        return _send(conf, _addr, _style, _context, _topic, msg, timeout)


def create_connection(conf, new=True):
    return Connection(conf)


def multicall(conf, context, topic, msg, timeout=None):
    """ Multiple calls """
    LOG.debug(_("RR MULTICALL %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    style, target, data = _multi_send(conf, "call", context, str(topic), msg,
        timeout=timeout)
    return data


def call(conf, context, topic, msg, timeout=None):
    """ Send a message, expect a response """
    LOG.debug(_("RR CALL %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    style, target, data = _multi_send(conf, "call", context, str(topic), msg,
        timeout=timeout)
    return data[-1]


def cast(conf, context, topic, msg):
    """ Send a message expecting no reply """
    LOG.debug(_("RR CAST %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    _multi_send(conf, "cast", context, str(topic), msg)


def fanout_cast(conf, context, topic, msg):
    """ Send a message to all listening and expect no reply """
    LOG.debug(_("FANOUT CAST %(msg)s") % {'msg': ' '.join(map(pformat,
        (topic, msg)))})
    _multi_send("fanout-cast", context, str(topic), msg)


def notify(conf, context, topic, msg):
    """
    Send notification event.
    Notifications are sent to topic-priority.
    This differs from the AMQP drivers which send to topic.priority.
    """
    # NOTE(ewindisch): dot-priority in rpc notifier does not
    # work with our assumptions.
    topic.replace('.', '-')
    cast(conf, context, topic, msg)


def cleanup():
    """Clean up resources in use by implementation."""

    # NOTE(ewindisch): All cleanup should be handled by
    # Connection.close().  Managing the connections
    # through a class variable/method would be ugly & broken.
    pass


def register_opts(conf):
    """
    Registration of options for this driver.
    """
    #NOTE(ewindisch): ZMQ_CTX and matchmaker
    # are initialized here as this is as good
    # an initialization method as any.

    # We memoize through these globals
    global ZMQ_CTX
    global matchmaker

    conf.register_opts(zmq_opts)

    # Don't re-set, if this method is called twice.
    if not ZMQ_CTX:
        ZMQ_CTX = zmq.Context(conf.rpc_zmq_contexts)
    if not matchmaker:
        module = globals()['mod_matchmaker']
        # split(conf.rpc_zmq_matchmaker, '.')
        constructor = getattr(module, 'MatchMakerLocalhost')
        matchmaker = constructor()
