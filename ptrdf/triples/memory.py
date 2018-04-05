#=======================================================================
#       Memory-only triplestore
#=======================================================================
# Support python2.2 which is latest on mythic-beasts which has bsddb
from __future__ import generators
from collections import OrderedDict

from . import Triples

class MemoryTriples(Triples):
    def __init__(self):
        self.d = {}                     # s|o -> [{p->{o->1}},{p->{s->1}}}]

    def add(self, subj,pred,obj):
        if subj in self.d:
            sx = self.d[subj]
        else:
            sx = self.d[subj] = [{},{}]
        if pred in sx[0]:
            px = sx[0][pred]
        else:
            px = sx[0][pred] = OrderedDict()
        if obj in px:
            pass;               # Already have triple
        else:
            px[obj] = True
            # Now add to reverse map
            if obj in self.d:
                ox = self.d[obj]
            else:
                ox = self.d[obj] = [{},{}]
            if pred in ox[1]:
                px = ox[1][pred]
            else:
                px = ox[1][pred] = OrderedDict()
            if subj in px:
                pass;           # Should not happen
            else:
                px[subj] = True

    def remove(self, subj,pred,obj):
        try:
            del self.d[subj][0][pred][obj]
        except KeyError: pass
        try:
            del self.d[obj][1][pred][subj]
        except KeyError: pass

    def iterator(self, subj=None,pred=None,obj=None):
        if subj is not None:
            if subj not in self.d: return
            sx = self.d[subj]
            if pred is not None:
                if pred not in sx[0]: return
                px = sx[0][pred]
                if obj is not None:
                    # Match: spo
                    if obj in px:
                        yield (subj,pred,obj)
                else:
                    # Match: sp?
                    for o in px.keys():
                        yield (subj,pred,o)
            elif obj is not None:
                # Match: s?o
                for (p,px) in sx[0].items():
                    if obj in px:
                        yield (subj,p,obj)
            else:
                # Match: s??
                for (p,px) in sx[0].items():
                    for o in px.keys():
                        yield (subj,p,o)
        elif obj is not None:
            if obj not in self.d: return
            ox = self.d[obj]
            if pred is not None:
                # Match: ?po
                if pred not in ox[1]: return
                px = ox[1][pred]
                for s in px.keys():
                    yield (s,pred,obj)
            else:
                # Match: ??o
                for (p,px) in ox[1].items():
                    for s in px.keys():
                        yield (s,p,obj)
        elif pred is not None:
            # Match: ?p?
            for (s,sx) in self.d.items():
                if pred in sx[0]:
                    px = sx[0][pred]
                    for o in px.keys():
                        yield (s,pred,o)
        else:
            # Match: ???
            for (s,sx) in self.d.items():
                for (p,px) in sx[0].items():
                    for o in px.keys():
                        yield (s,p,o)
        return

    def iter_sp2o(self, subj,pred):
        if subj in self.d:
            sx = self.d[subj]
            if pred in sx[0]:
                px = sx[0][pred]
                for o in px.keys():
                    yield o

    def iter_po2s(self, pred,obj):
        if obj in self.d:
            ox = self.d[obj]
            if pred in ox[1]:
                px = ox[1][pred]
                for s in px.keys():
                    yield s

    def iter_s2p(self, subj):
        if subj in self.d:
            sx = self.d[subj]
            for (p,px) in sx[0].items():
                if px:
                    yield p

    def test(self, subj,pred,obj):
        if subj in self.d:
            sx = self.d[subj]
            if pred in sx[0]:
                px = sx[0][pred]
                if obj in px:
                    return True
        return False

    def commit(self):
        pass;                   # All updates instantaneous

    def rollback(self):
        raise Exception("No rollback possible")

# End
