from nova import block_device
from nova import db
from nova import exception
from nova import flags
from nova import utils
from nova import log as logging


FLAGS = flags.FLAGS
LOG = logging.getLogger(__name__)

CONTEXT = nova.context.get_admin_context()

def _call(self, name):
    # Curry method into the rpc call, only accept args to def
    def rpccall(args):
        rpc.call(CONTEXT, FLAGS.database_topic, {
            "method": name,
            "args": args
        })
    return rpccall

def __getattr__(self, name):
    return self._call(name)
