#=======================================================================
#       Triple store using string dict
#=======================================================================
from . import Triples
import re

class DictTriples(Triples):
    def __init__(self):
        self.db = {}
        self.features = 'pP'

    def iterator(self, subj,pred,obj):
        # Most likely to have predicate defined
        if pred is not None:
            p = self._get('i'+pred)
            if not p: return
            if subj is not None:
                s = self._get('i'+subj)
                if not s: return
                if obj is not None:
                    # Match: spo
                    o = self._get('i'+obj)
                    if not o: return
                    if self._get("t%s,%s,%s" % (s,p,o)) is not None:
                        yield (subj,pred,obj)
                    return
                else:
                    # Match: sp?
                    oids = self._geta("o%s,%s" % (s,p))
                    for o in oids:
                        obj = self._get('v'+o)
                        yield (subj,pred,obj)
                    return
            elif obj is not None:
                # Match: ?po
                o = self._get('i'+obj)
                if not o: return
                sids = self._geta("s%s,%s" % (p,o))
                for s in sids:
                    subj = self._get('v'+s)
                    yield (subj,pred,obj)
                return
            else:
                # Match: ?p? -> generic
                pass

        elif subj is not None:
            s = self._get('i'+subj)
            if not s: return
            if obj is not None:
                # Match: s?o
                o = self._get('i'+obj)
                if not o: return
                if 'p' in self.features:
                    pids = self._geta('p'+s)
                    for p in pids:
                        if self._get('t%s,%s,%s' % (s,p,o)) is not None:
                            pred = self._get('v'+p)
                            yield (subj,pred,obj)
                    return
                else:
                    # -> generic
                    pass
            else:
                # Match: s??
                if 'p' in self.features:
                    pids = self._geta('p'+s)
                    for p in pids:
                        pred = self._get('v'+p)
                        for o in self._geta("o%s,%s" % (s,p)):
                            obj = self._get('v'+o)
                            yield (subj,pred,obj)
                    return
                else:
                    # generic
                    pass
        elif obj is not None:
            # Match: ??o
            o = self._get('i'+obj)
            if not o: return
            if 'P' in self.features:
                pids = self._geta('P'+o)
                for p in pids:
                    pred = self._get('v'+p)
                    sids = self._geta("s%s,%s" % (p,o))
                    for s in sids:
                        subj = self._get('v'+s)
                        yield (subj,pred,obj)
                return
            else:
                # generic
                pass
        else:
            # Match: ??? -> generic
            # generic
            pass

        # Create regexp to match any known items
        if subj is None: s = r'\d+'
        if pred is None: p = r'\d+'
        if obj  is None: o = r'\d+'
        rxs = '^t(%s),(%s),(%s)' % (s,p,o)
        rx = re.compile(rxs)
        for (k,v) in self._iteritems():
            m = rx.match(k)
            if m is not None:
                subj = self._get('v'+m.group(1))
                pred = self._get('v'+m.group(2))
                obj  = self._get('v'+m.group(3))
                yield (subj,pred,obj)
        # FIXME - does not check for modifications
        pass

    def add(self, subj,pred,obj):
        s = self._id(subj)
        p = self._id(pred)
        o = self._id(obj)
        tkey = "t%s,%s,%s" % (s,p,o)
        if self._get(tkey):
            pass;               # Already there
        else:
            self._set(tkey, '1')

            okey = "o%s,%s" % (s,p)
            if 'p' in self.features:
                # Keeping predicate lists
                if not self._testa(okey):
                    # New predicate for subject
                    self._adda('p'+s, p)
            self._adda(okey, o)

            skey = "s%s,%s" % (p,o)
            if 'P' in self.features:
                # Keeping predicate lists
                if not self._testa(skey):
                    # New predicate for object
                    self._adda('P'+o, p)
            self._adda(skey, s)

    def commit(self):
        pass

    def disconnect(self):
        pass

#-----------------------------------------------------------------------
#       Implementation helpers
#-----------------------------------------------------------------------
    def _id(self, value):
        '''Convert URI or literal to id.
        '''
        id = self._get('i'+value)
        if id is None:
            last = self._get('l')
            if last is None:
                last = '0'
            id = str(int(last) + 1)
            self._set('l', id)
            self._set('i'+value, id)
            self._set('v'+id, value)
        return id

    def _get(self, key):
        '''Fetch the value for given key, or None if not found.
        '''
        val = self.db.get(key, None)
        return val

    def _geta(self, key):
        val = self._get(key)
        if val is None:
            return []
        else:
            return val.split(' ')

    def _set(self, key,val):
        self.db[key] = val

    def _seta(self, key,values):
        if values:
            self.db[key] = ' '.join(values)
        else:
            del self.db[key]

    def _adda(self, key,new):
        vals = self._get(key)
        if vals is not None:
            self.db[key] = vals+' '+new
        else:
            self.db[key] = new

    def _testa(self,key):
        vals = self._get(key)
        return (vals is not None)

    def _iteritems(self):
        for kv in self.db.iteritems():
            yield kv

# End
