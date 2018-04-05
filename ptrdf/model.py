#=======================================================================
#       RDF-model
#=======================================================================
class Model(object):
    """RDF model containing a number of statements."""
    def __init__(self, triples):
        self.triples = triples
        self.dbrdftype = 'rdf:type'
        self.objmap = {}                # uri -> Obj

    def as_obj(self,uri):
        """Get Obj for given URI"""
        # See if already known
        obj = self.objmap.get(uri)
        if obj: return obj

        # Create new object, as yet unblessed
        obj = self.objmap[uri] = Obj(uri,self)
        return obj

    def iter_sp2o(self, subj,pred):
        tsubj = self._in(subj)
        tpred = self._inpred(pred)
        for tobj in self.triples.iter_sp2o(tsubj,tpred):
            yield self._out(tobj)

    def all_sp2o(self, subj,pred):
        return list(self.iter_sp2o(subj,pred))

    def sp2o(self, subj,pred):
        try:
            it = self.iter_sp2o(subj,pred)
            return next(it)
        except StopIteration:
            return None

    def iter_po2s(self, pred,obj):
        tpred = self._inpred(pred)
        tobj = self._in(obj)
        for tsubj in self.triples.iter_po2s(tpred,tobj):
            yield self._out(tsubj)

    def all_po2s(self, pred,obj):
        return list(self.iter_po2s(pred,obj))

    def po2s(self, pred,obj):
        try:
            it = self.iter_po2s(pred,obj)
            return next(it)
        except StopIteration:
            return None

    def test(self, subj,pred,obj):
        tsubj = self._in(subj)
        tpred = self._inpred(pred)
        tobj = self._in(obj)
        return self.triples.test(tsubj,tpred,tobj)
        
    def _in(self,val):
        if type(val) is type(''):
            return '='+val
        else:
            uri = val.uri
            return uri

    def _inpred(self, val):
        if type(val) is type(''):
            return val
        else:
            return val.uri

    def _out(self,tval):
        '''Convert string from triplestore to object or literal.
        '''
        if tval[0] is '=':
            return tval[1:]
        else:
            return self.as_obj(tval)

class Obj(object):
    """URI reference"""
    def __init__(self, uri,model):
        self.uri = uri
        self.model = model

    def __repr__(self):
        return "<Obj '%s'>" % self.uri

    def test(self, pred,obj):
        return self.model.test(self, pred, obj)

    def has_type(self, type):
        model = self.model
        return self.test(model.dbrdftype, model.as_obj(type))

    def attr(self, pred):
        return self.model.sp2o(self,pred)

    def iter_attr(self, pred):
        for obj in self.model.iter_sp2o(self, pred):
            yield obj

    def all_attr(self, pred):
        return self.model.all_sp2o(self, pred)

    def rattr(self, pred):
        return self.model.po2s(pred,self)

    def iter_rattr(self, pred):
        for subj in self.model.iter_po2s(pred,self):
            yield subj

    def all_rattr(self, pred):
        return self.model.all_po2s(pred,self)

# End
