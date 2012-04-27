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

from nova import block_device
from nova import db
from nova import exception
from nova import flags
from nova import utils
from nova import log as logging


FLAGS = flags.FLAGS
LOG = logging.getLogger(__name__)

CONTEXT = nova.context.get_admin_context()

#TODO(ewindisch): create following StrOpts:
# - database_write_topic, default=database.localhost or database?
# - database_topic, default=database

def _call(self, name, topic=None):
    topic = topic or FLAGS.database_topic

    # Curry method into the rpc call, only accept args to def
    def rpccall(args):
        rpc.call(CONTEXT, topic, {
            "method": name,
            "args": args
        })
    return rpccall

def __getattr__(self, name):
    write_cmds = ( 'update',
                   'write',
                   'associate',
                   'create',
                   'delete' )
    for cmd in write_cmds:
        if name.find(cmd) > -1:
        	return self._call(name, FLAGS.database_write_topic)
    return self._call(name)
