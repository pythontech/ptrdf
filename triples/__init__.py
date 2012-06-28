#=======================================================================
#       $Id: __init__.py,v 1.1 2007/09/25 12:23:12 pythontech Exp pythontech $
#       Generic Triplestore
#=======================================================================
# Support python2.2 which is latest on mythic-beasts which has bsddb
from __future__ import generators

class Triples:
    '''Virtual triplestore class.
    '''

    def __init__(self):
        pass

    def copy_in(self, other):
        '''Add contents of another triplestore to this one.
        '''
        for (subj,pred,obj) in other.iterator():
            self.add(subj,pred,obj)

    def iter_query(self, subj,pred,obj):
	'''Iterate over query returning unspecified items.
	'''
	for (s,p,o) in self.iterator(subj,pred,obj):
	    res = []
	    if subj is None: res.append(s)
	    if pred is None: res.append(p)
	    if obj  is None: res.append(o)
	    yield tuple(res)

    def iter_sp2o(self, subj,pred):
        '''Iterate over objects for given subject and predicate.
        '''
        for (s,p,obj) in self.iterator(subj,pred,None):
            yield obj

    def iter_po2s(self, pred,obj):
        '''Iterate over subjects for given predicate and object.
        '''
        for (subj,p,o) in self.iterator(None,pred,obj):
            yield subj

    def iter_s2p(self, subj):
        '''Iterate over predicates for given subject.
        '''
        seen = {}
        for (s,pred,obj) in self.iterator(subj,None,None):
            if pred not in seen:
                seen[pred] = True
                yield pred

    def iter_o2p(self, obj):
        '''Iterate over predicates for given object.
        '''
        seen = {}
        for (subj,pred,o) in self.iterator(None,None,obj):
            if pred not in seen:
                seen[pred] = True
                yield pred

# Normal user-callable methods

    def test(self, subj,pred,obj):
        '''Test if triple is in triplestore.
        '''
        tst = _first(self.iterator(subj,pred,obj))
	return (tst is not None)

    def query(self, subj,pred,obj):
	'''Return one query result (if any).
	'''
	return _first(self.iter_query(subj,pred,obj))

    def all_query(self, subj,pred,obj):
	'''Return all query results.
	'''
	return list(self.iter_query(subj,pred,obj))

    def sp2o(self, subj,pred):
        '''Return one object (if any) for given subject and predicate.
        '''
        return _first(self.iter_sp2o(subj,pred))

    def all_sp2o(self, subj,pred):
        '''Return all objects for given subject and predicate.
        '''
        return list(self.iter_sp2o(subj,pred))

    def po2s(self, pred,obj):
        '''Return one subject (if any) for given predicate and object.
        '''
        return _first(self.iter_po2s(pred,obj))

    def all_po2s(self, pred,obj):
        '''Return all subjects for given predicate and object.
        '''
        return list(self.iter_po2s(pred,obj))

    def s2p(self, subj):
	'''Return one predicate (if any) for given subject.
	'''
	return _first(self.iter_s2p(subj))

    def all_s2p(self, subj):
        '''Return all predicates for given subject.
        '''
        return list(self.iter_s2p(subj))

    def s2p(self, obj):
	'''Return one predicate (if any) for given object.
	'''
	return _first(self.iter_o2p(obj))

    def all_o2p(self, obj):
        '''Return all predicates for given object.
        '''
        return list(self.iter_o2p(obj))

    def commit(self):
        raise NotImplementedError, repr(type(self))+" does not implement 'commit'"

    def rollback(self):
        raise NotImplementedError, repr(type(self))+" does not implement 'rollback'"

def _first(it):
    '''Return first (if any) item from iterator.
    '''
    try:
        res = it.next()
    except StopIteration:
        res = None
    return res

# End
