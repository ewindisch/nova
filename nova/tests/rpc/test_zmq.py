# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 Cloudscaling Group, Inc.
# All Rights Reserved.
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
"""
Unit Tests for remote procedure calls using zeromq
"""

import socket

from nova import log as logging
from nova import test
from nova.tests.rpc import common

try:
    from eventlet.green import zmq
    from nova.rpc import impl_zmq
except ImportError:
    zmq = None
    impl_zmq = None

LOG = logging.getLogger(__name__)


class _RpcZmqBaseTestCase(common.BaseRpcTestCase):
    def tearDown(self):
        if impl_zmq:
            super(_RpcZmqBaseTestCase, self).tearDown()

    @test.skip_if(zmq is None, "Test requires zmq")
    def __getattr__(self, name):
        super(_RpcZmqBaseTestCase, self).getattr(name)


class RpcZmqBaseTopicTestCase(_RpcZmqBaseTestCase):
    def setUp(self):
        self.rpc = impl_zmq

        if impl_zmq:
            super(_RpcZmqBaseTestCase, self).setUp()


class RpcZmqDirectTopicTestCase(_RpcZmqBaseTestCase):
    def setUp(self):
        self.rpc = impl_zmq

        if impl_zmq:
            super(_RpcZmqBaseTestCase, self).setUp(
                  topic='test.localhost',
                  topic_nested='nested.localhost')
