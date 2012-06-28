#=======================================================================
#	$Id$
#=======================================================================
import triples
import urllib2
import sys
import logging

_log = logging.getLogger('triples.web')

class WebTriples(triples.Triples):
    '''Triple-store acccesed via a web URL'''
    def __init__(self, weburl):
	self.weburl = weburl
	self.agent = urllib2.build_opener()

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
	    subj, pred, obj = map(ux, line.split())
	    yield subj, pred, obj

    def _get(self, query={}):
	url = self.weburl
	if query:
	    url += '?' + urlencode(query)
	try:
	    doc = self.agent.open(url)
	except urllib2.HTTPError, e:
	    print >>sys.stderr, 'HTTPError %d' % e.code
	    raise
	status = doc.getcode()
	_log.debug('status %d',status)
	#info = doc.info()
	#print info.keys()
	#for k in info:
	#    print k, info[k]
	return doc

#-----------------------------------------------------------------------
#	Helpers
#-----------------------------------------------------------------------
def urlencode(vars):
    return '&'.join(['%s=%s' % (uq(n), uq(v)) for n, v in vars.items()])
#    return '&'.join(map(lambda kv: '='.join(map(uq, kv)),
#                        vars.items()))


def uq(step):
    return urllib2.quote(step, safe='')

ux = urllib2.unquote

