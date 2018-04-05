#=======================================================================
#       Triplestore accessed via a web query
#=======================================================================
from . import Triples
try:
    from urllib.error import HTTPError
    from urllib.request import build_opener as _build_opener
    from urllib.parse import quote as _quote, unquote as _unquote
except ImportError:                     # PY2
    from urllib2 import HTTPError, build_opener as _build_opener
    from urllib import quote as _quote, unquote as _unquote
import logging

_log = logging.getLogger(__name__)

class WebTriples(Triples):
    '''Triple-store acccesed via a web URL'''
    def __init__(self, weburl):
        self.weburl = weburl
        self.agent = _build_opener()

    def iterator(self, subj=None, pred=None, obj=None):
        query = {}
        if subj is not None:
            query['s'] = subj
        if pred is not None:
            query['p'] = pred
        if obj is not None:
            query['o'] = obj
        doc = self._get(query)
        for line in doc:
            subj, pred, obj = [ux(q)  for q in line.split()]
            yield subj, pred, obj

    def _get(self, query={}):
        url = self.weburl
        if query:
            url += '?' + urlencode(query)
        try:
            doc = self.agent.open(url)
        except HTTPError as e:
            _log.warning('HTTPError %d' % e.code)
            raise
        status = doc.getcode()
        _log.debug('status %d',status)
        return doc

#-----------------------------------------------------------------------
#       Helpers
#-----------------------------------------------------------------------
def urlencode(vars):
    return '&'.join(['%s=%s' % (uq(n), uq(v))
                     for n, v in vars.items()])

def uq(step):
    return _quote(step, safe='')

ux = _unquote

