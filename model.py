#=======================================================================
#       $Id: model.py,v 1.1 2009/06/25 15:16:00 chah Exp $
#	RDF-model
#=======================================================================
class Model:
    def __init__(self, triples):
        self.triples = triples
        self.dbrdftype = 'rdf:type'
        self.objmap = {}

    def as_obj(self,uri):
        # See if already known
        obj = self.objmap.get(uri)
        if obj: return obj

        # Create new object, as yet unblessed
        obj = self.objmap[uri] = Obj(uri,self)
        return obj

    def iter_sp2o(self, subj,pred):
        tsubj = self._insub(subj)
        tpred = self._inpred(pred)
        for tobj in self.triples.iter_sp2o(tsubj,tpred):
            yield self._out(tobj)

    def all_sp2o(self, subj,pred):
        return list(self.iter_sp2o(subj,pred))

    def sp2o(self, subj,pred):
        try:
            return self.iter_sp2o(subj,pred).next()
        except StopIteration:
            return None

    def _in(self,val):
        if type(val) is type(''):
            return '='+val
        else:
            uri = val.uri
            return uri

    def _out(self,tval):
        '''Convert string from triplestore to object or literal.
        '''
        if tval[0] is '=':
            return tval[1:]
        else:
            return self.as_obj(tval)

class Obj:
    def __init__(self, uri,model):
        self.uri = uri
        self.model = model

    def test(self, pred,obj):
        return self.model.test(self,pred,obj)

    def has_type(self, type):
        model = self.model
        return model.test(self,model.dbrdftype, URI(type))

    def attr(self, pred):
        return self.model.sp2o(self,pred)

    def rattr(self, pred):
        return self.model.po2s(pred,self)

# End
