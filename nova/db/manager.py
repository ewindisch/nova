# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 Cloudscaling Group
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

"""Handles all database calls

The :py:class:`DatabaseManager` class is a :py:class:`nova.manager.Manager` that
handles RPC calls relating to database operations.

**Related Flags**

:database_driver:  Name of class that is used to handle databases, loaded
                  by :func:`nova.utils.import_object`

"""

import contextlib
import functools
import os
import socket
import sys
import tempfile
import time
import traceback

from eventlet import greenthread

import nova.context
from nova import exception
from nova import flags
from nova import log as logging
from nova import manager
from nova.notifier import api as notifier
from nova.openstack.common import cfg
from nova import rpc
from nova import utils


database_opts = [
    ]

FLAGS = flags.FLAGS
FLAGS.register_opts(database_opts)

LOG = logging.getLogger(__name__)


class DatabaseManager(manager.Manager):
    """Manages the running instances from creation to destruction."""

    def __init__(self, database_driver=None, *args, **kwargs):
        self.context = nova.context.get_admin_context()

        # NOTE(ewindisch): borrowed import code from compute
        # TODO(vish): sync driver creation logic with the rest of the system
        #             and re-document the module docstring
        if not database_driver:
            database_driver = FLAGS.database_driver
        try:
            self.driver = utils.check_isinstance(
                                        utils.import_object(database_driver),
                                        driver.DatabaseDriver)
        except ImportError as e:
            LOG.error(_("Unable to load the database driver: %s") % (e))
            sys.exit(1)

        super(DatabaseManager, self).__init__(service_name="database",
                                             *args, **kwargs)


    def init_host(self):
        pass

    def _call(self, name):
        # Curry method into the rpc call, only accept args to def
        def rpccall(args):
            rpc.call(self.context, self.topic, {
                "method": name,
                "args": args
            })
        return rpccall

    def __getattr__(self, name):
        return self._call(name)
