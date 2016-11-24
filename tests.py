#=======================================================================
#       $Id: tests.py,v 1.1 2009/06/25 15:15:33 chah Exp $
#       Test suite
#       Converted from perl PythonTech-Triples package
#=======================================================================
import unittest
import re

# From 04_memquery.t
class TestMemoryTriples(unittest.TestCase):

    def test_memory(self):
        from ptrdf.triples.memory import MemoryTriples
        t = MemoryTriples()
        t.add('a','b','c')
        t.add('a','b','d')
        t.add('a','e','c')
        self.assertEqual(t.test('a','b','c'), True, 'simple test')
        self.assertEqual(t.test('a','b','d'), True, 'simple test')
        self.assertEqual(t.test('a','b','a'), False, 'test no, resource exists')
        self.assertEqual(t.test('x','b','c'), False)
        self.assertEqual(t.test('a','y','c'), False)
        self.assertEqual(t.test('a','b','z'), False)
        self.assertEqual(qt(t.all_query('a','b',None)), 'c;d')
        self.assertEqual(qt(t.all_query('a',None,'c')), 'b;e', 'query s?p');
        self.assertEqual(qt(t.all_query('a',None,None)), 'b,c;b,d;e,c', 'query s??');
        self.assertEqual(qt(t.all_query(None,'b','c')), 'a', 'query ?po');
        self.assertEqual(qt(t.all_query(None,'b',None)), 'a,c;a,d', 'query ?p?');
        self.assertEqual(qt(t.all_query(None,None,'d')), 'a,b', 'query ??o');
        self.assertEqual(qt(t.all_query(None,None,None)), 'a,b,c;a,b,d;a,e,c', 'query ???');
        self.assertEqual(j(t.all_sp2o('a','b')), 'c,d', 'sp2o');
        self.assertEqual(j(t.all_sp2o('a','y')), '', 'sp2o no match');
        self.assert_(t.sp2o('a','b') in ['c','d'], 'sp2o scalar');
        self.assertEqual(j(t.all_po2s('b','d')), 'a', 'po2s');
        self.assertEqual(t.po2s('b','d'), 'a', 'po2s scalar');
        self.assertEqual(j(t.all_po2s('b','z')), '', 'po2s no match');
        self.assertEqual(j(t.all_s2p('a')), 'b,e', 's2p');
        self.assertEqual(j(t.all_s2p('c')), '', 's2p no match');

# From 01_newdb.t
class TestNewDb(unittest.TestCase):

    def test_newdb(self):
        from ptrdf.triples.db import DBTriples
        dbname = 'new01.db'
        t = DBTriples(dbname,'n')
        t.disconnect()
        # defined t && -f dbname
        # -s dbname != 0
        self.assertEqual(dbread(dbname), '', 'nothing written')

# From 02_dbadd.t
class TestDbAdd(unittest.TestCase):

    def test_dbadd(self):
        from ptrdf.triples.db import DBTriples
        dbname = 'new02.db'
        t = DBTriples(dbname,'n')
        t.add('foo','bar','baz')
        t.add('foo','bar','quux')
        t.commit()
        t.disconnect()

        expected = ''.join(map(lambda x: x+'\n',
                               sorted(('v1 = foo', 'ifoo = 1',
                                       'v2 = bar', 'ibar = 2',
                                       'v3 = baz', 'ibaz = 3',
                                       'v4 = quux', 'iquux = 4',
                                       'l = 4',
                                       't1,2,3 = 1',
                                       't1,2,4 = 1',
                                       'o1,2 = 3+4',
                                       's2,3 = 1',
                                       's2,4 = 1'))))
        self.assertEqual(dbread(dbname), expected, 'db data for 2 triples')

# From 03_dbquery.t
class TestDbQuery(unittest.TestCase):

    def test_dbquery(self):
        from ptrdf.triples.db import DBTriples
        dbname = 'new03.db'
        dbwrite(dbname,'''v1 = a
v2 = b
v3 = c
v4 = d
v5 = e
ia = 1
ib = 2
ic = 3
id = 4
ie = 5
l = 5
t1,2,3 = 1
t1,2,4 = 1
t1,5,3 = 1
o1,2 = 3+4
o1,5 = 3
s2,3 = 1
s2,4 = 1
s5,3 = 1
''')
        t = DBTriples(dbname,'r')
        self.assertEqual(t.test('a','b','c'), True, 'simple test')
        self.assertEqual(t.test('a','b','d'), True, 'simple test')
        self.assertEqual(t.test('a','b','a'), False, 'test no, resource exists')
        self.assertEqual(t.test('x','b','c'), False, 'test no, subject unknown')
        self.assertEqual(t.test('a','y','c'), False, 'test no, predicate unknown')
        self.assertEqual(t.test('a','b','z'), False, 'test no, object unknown')
        self.assertEqual(qt(t.all_query('a','b',None)), 'c;d', 'query sp?')
        self.assertEqual(qt(t.all_query('a',None,'c')), 'b;e', 'query s?p')
        self.assertEqual(qt(t.all_query('a',None,None)), 'b,c;b,d;e,c', 'query s??')
        self.assertEqual(qt(t.all_query(None,'b','c')), 'a', 'query ?po')
        self.assertEqual(qt(t.all_query(None,'b',None)), 'a,c;a,d', 'query ?p?')
        self.assertEqual(qt(t.all_query(None,None,'d')), 'a,b', 'query ??o')
        self.assertEqual(qt(t.all_query(None,None,None)), 'a,b,c;a,b,d;a,e,c', 'query ???')
        self.assertEqual(j(t.all_sp2o('a','b')), 'c,d', 'sp2o')
        self.assertEqual(j(t.sp2o('a','y')), '', 'sp2o no match')
        self.assertEqual(t.sp2o('a','b') in ('c','d'), True, 'sp2o scalar')
        self.assertEqual(j(t.all_po2s('b','d')), 'a', 'po2s')
        self.assertEqual(j(t.po2s('b','d')), 'a', 'po2s scalar')
        self.assertEqual(j(t.all_po2s('b','z')), '', 'po2s no match')
        self.assertEqual(j(t.all_s2p('a')), 'b,e', 's2p')
        self.assertEqual(j(t.all_s2p('c')), '', 's2p no match')

#-----------------------------------------------------------------------
#       Utility functions
#-----------------------------------------------------------------------
def qt(qres):
    '''Given query results (list of tuples), return string form
    with results sorted
    e.g. [('a','d'),('a','b')] -> 'a,b;a,d'
    '''
    return ';'.join(sorted(map(lambda x: ','.join(x),
                               qres)))

def j(seq):
    if seq is None: return ''
    return ','.join(sorted(seq))

def sorted(seq):
    res = list(seq)
    res.sort()
    return res

def dbread(filename):
    import bsddb
    db = bsddb.btopen(filename,'r')
    dump = ''
    for key in sorted(db.keys()):
        dump += "%s = %s\n" % (e(key), e(db[key]))
    return dump

def dbwrite(filename,data):
    import bsddb
    db = bsddb.btopen(filename,'n')
    if type(data) is type({}):
        for (k,v) in data.items():
            db[k] = db[v]
    elif type(data) is type(''):
        for line in data.split('\n'):
            m = re.match(r'^(\S*) = (\S*)',
                         line)
            if m:
                db[d(m.group(1))] = d(m.group(2))
    else:
        raise TypeError("dbwrite expects dict or string")
    db.close()

def e(text):
    text = re.sub(r'\%|\+|[^\x20-\x7e]',
                  lambda x: '%%%02X' % (ord(x.group())),
                  text)
    text = text.replace(' ','+')
    return text

def d(text):
    text = text.replace('+',' ')
    text = re.sub(r'%([0-9A-Fa-f]{2})',
                  lambda x: chr(x.group(1),16),
                  text)
    return text

unittest.main()
