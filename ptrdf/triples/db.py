#=======================================================================
#       Triplestore using Berkeley DB file
#=======================================================================
# Support python2.2 which is latest on mythic-beasts which has bsddb
from __future__ import generators

from .dict import DictTriples
import bsddb

class DBTriples(DictTriples):
    def __init__(self, filename, access='r'):
        self.filename = filename
        self.access = access
        self.db = bsddb.btopen(filename,access)
        self.features = self._get('f') or ''

    def _get(self, key):
        if not self.db.has_key(key):
            return None
        return self.db[key]

    def _iteritems(self):
        try:
            x = self.db.first()
            yield x
            while True:
                x = self.db.next()
                yield x
        except KeyError:
            raise StopIteration

    def disconnect(self):
        self.db.close()

# End
