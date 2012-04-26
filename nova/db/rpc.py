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
