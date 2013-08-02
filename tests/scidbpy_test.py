"""
This file is part of scidb4py.  scidb4py is free software: you can
redistribute it and/or modify it under the terms of the GNU General Public
License as published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

Copyright (c) 2013, Artyom Smirnov <artyom_smirnov@icloud.com>
"""

import unittest
import os
from scidb4py import Connection
from scidb4py.types import *

scidb_host = os.getenv('SCIDB_HOST', 'localhost')


class Cleanup(object):
    def __init__(self, array_name):
        self._array_name = array_name

    def drop(self):
        Basic.connection.execute("drop array %s" % self._array_name)
        Basic.connection.complete()
        pass

    def __call__(self, function):
        def func(*args, **kwargs):
            try:
                function(*args, **kwargs)
                self.drop()
            except:
                if Basic.connection.active:
                    Basic.connection.cancel()
                self.drop()
                raise
        return func


class Basic(unittest.TestCase):
    connection = None

    #noinspection PyBroadException
    @classmethod
    def setUpClass(cls):
        cls.connection = Connection(scidb_host)
        cls.connection.open()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def test_schema(self):
        a = self.connection.execute(
            "select * from array(<a:int8, b:int16 null>[x=0:3,2,0, y=0:2,1,0], '[[]]')")

        self.assertEqual(len(a.schema.attributes), 3)

        self.assertEqual(a.schema.attributes[0].name, 'a')
        self.assertEqual(a.schema.attributes[1].name, 'b')

        self.assertEqual(a.schema.attributes[0].type, TID_INT8)
        self.assertEqual(a.schema.attributes[1].type, TID_INT16)
        self.assertEqual(a.schema.attributes[2].type, TID_INDICATOR)

        self.assertFalse(a.schema.attributes[0].nullable)
        self.assertTrue(a.schema.attributes[1].nullable)
        self.assertFalse(a.schema.attributes[2].nullable)

        self.assertFalse(a.schema.attributes[0].empty_indicator)
        self.assertFalse(a.schema.attributes[1].empty_indicator)
        self.assertTrue(a.schema.attributes[2].empty_indicator)

        self.assertEqual(len(a.schema.dimensions), 2)

        self.assertEqual(a.schema.dimensions[0].name, 'x')
        self.assertEqual(a.schema.dimensions[1].name, 'y')

        self.assertEqual(a.schema.dimensions[0].start, 0)
        self.assertEqual(a.schema.dimensions[0].end, 3)

        self.assertEqual(a.schema.dimensions[1].start, 0)
        self.assertEqual(a.schema.dimensions[1].end, 2)

        self.assertEqual(a.schema.dimensions[0].chunk_interval, 2)

        self.assertEqual(a.schema.dimensions[1].chunk_interval, 1)

        self.assertEqual(a.schema.dimensions[0].mapping_array_name, '')
        self.assertEqual(a.schema.dimensions[1].mapping_array_name, '')

    @Cleanup("A")
    def test_non_selective(self):
        a = self.connection.execute("create array A <a:int32 null> [x=0:2,3,0, y=0:2,3,0]")
        self.assertEqual(a, None)
        self.connection.complete()

        a = self.connection.execute("select * from array(A, '[[1,2,3][4,5,6][7,8,9]]')")
        self.assertNotEqual(a, None)
        self.assertTrue(self.connection.result.selective)
        self.connection.complete()

    def test_int8(self):
        a = self.connection.execute("select * from array(<a:int8, b:int8 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        self.assertFalse(a.schema.attributes[0].nullable)
        self.assertTrue(a.schema.attributes[1].nullable)

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_int16(self):
        a = self.connection.execute(
            "select * from array(<a:int16, b:int16 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_int32(self):
        a = self.connection.execute(
            "select * from array(<a:int32, b:int32 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_int64(self):
        a = self.connection.execute(
            "select * from array(<a:int64, b:int64 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_uint8(self):
        a = self.connection.execute(
            "select * from array(<a:uint8, b:uint8 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_uint16(self):
        a = self.connection.execute(
            "select * from array(<a:uint16, b:uint16 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_uint32(self):
        a = self.connection.execute(
            "select * from array(<a:uint32, b:uint32 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_uint64(self):
        a = self.connection.execute(
            "select * from array(<a:uint64, b:uint64 null>[x=0:3,2,0], '[(1,2)()][(4,5)(6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1 b:2, x:2 a:4 b:5, x:3 a:6 b:None, ')

    def test_float(self):
        a = self.connection.execute(
            "select * from array(<a:float, b:float null>[x=0:3,2,0], '[(1.1,2.2)()][(4.4,5.5)(6.6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res,
                         'x:0 a:1.10000002384 b:2.20000004768, x:2 a:4.40000009537 b:5.5, x:3 a:6.59999990463 b:None, ')

    def test_double(self):
        a = self.connection.execute(
            "select * from array(<a:double, b:double null>[x=0:3,2,0], '[(1.1,2.2)()][(4.4,5.5)(6.6,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:1.1 b:2.2, x:2 a:4.4 b:5.5, x:3 a:6.6 b:None, ')

    def test_string(self):
        a = self.connection.execute(
            "select * from array(<a:string, b:string null>[x=0:3,2,0], '[(foo, bar)()][(baz, quux)(zzz,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:foo b:bar, x:2 a:baz b:quux, x:3 a:zzz b:None, ')

    def test_long_string(self):
        a = self.connection.execute(
            "select * from array(<a:string, b:string null>[x=0:3,2,0], '[(%s, %s)()][(%s, %s)(%s,null)]') " %\
            ("a" * 500, "b" * 500, "c" * 500, "d" * 500, "e" * 500))

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:%s b:%s, x:2 a:%s b:%s, x:3 a:%s b:None, ' %\
                              ("a" * 500, "b" * 500, "c" * 500, "d" * 500, "e" * 500))

    def test_char(self):
        a = self.connection.execute(
            "select * from array(<a:char, b:char null>[x=0:3,2,0], '[(a, b)()][(c, d)(e,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:a b:b, x:2 a:c b:d, x:3 a:e b:None, ')

    def test_bool(self):
        a = self.connection.execute(
            "select * from array(<a:bool, b:bool null>[x=0:3,2,0], '[(true, false)()][(false, true)(false,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:True b:False, x:2 a:False b:True, x:3 a:False b:None, ')

    @Cleanup("A")
    def test_mapping_arrays(self):
        r = self.connection.execute("create array A <x:int64> [a(string)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:string>[x=0:2,3,0], '[aaa,bbb,ccc]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A")
        res = ''
        for pos, val in r:
            res += str(r.nid_mapping('a')[pos['a']]) + str(pos) + str(val) + "\n"
        self.connection.complete()
        self.assertEqual(res, "{0L: 'aaa'}{u'a': 0L, 0: 0L}{0: 0, u'x': 0}\n"
                              "{1L: 'bbb'}{u'a': 1L, 0: 1L}{0: 1, u'x': 1}\n"
                              "{2L: 'ccc'}{u'a': 2L, 0: 2L}{0: 2, u'x': 2}\n")

    def test_compression_zlib(self):
        a = self.connection.execute(
            "select * from array(<a:int32 compression 'zlib'>[x=0:3,2,0], '[1,2][3,4]')")

        res = ''
        for pos, val in a:
            res += ('x:%d a:%s, ' % (pos['x'], val['a']))

        self.assertEqual(res, 'x:0 a:1, x:1 a:2, x:2 a:3, x:3 a:4, ')

    def test_compression_bzlib(self):
        a = self.connection.execute(
            "select * from array(<a:string compression 'bzlib'>[x=0:3,2,0],"
            "'[%s,%s][%s,%s]')" % ("a" * 200, "b" * 300, "c" * 200, "d" * 300))

        res = ''
        for pos, val in a:
            res += ('x:%d a:%s, ' % (pos['x'], val['a']))

        self.assertEqual(res, 'x:0 a:%s, x:1 a:%s, x:2 a:%s, x:3 a:%s, ' % ("a" * 200, "b" * 300, "c" * 200, "d" * 300))

    def test_compression_null_filter(self):
        a = self.connection.execute(
            "select * from array(<a:int32 compression 'null filter'>[x=0:3,2,0], '[1,2][3,4]')")

        res = ''
        for pos, val in a:
            res += ('x:%d a:%s, ' % (pos['x'], val['a']))

        self.assertEqual(res, 'x:0 a:1, x:1 a:2, x:2 a:3, x:3 a:4, ')

    def test_compression_rle(self):
        a = self.connection.execute(
            "select * from array(<a:int32 compression 'rle'>[x=0:3,2,0], '[1,2][3,4]')")

        res = ''
        for pos, val in a:
            res += ('x:%d a:%s, ' % (pos['x'], val['a']))

        self.assertEqual(res, 'x:0 a:1, x:1 a:2, x:2 a:3, x:3 a:4, ')

    def test_compression_bitmap_encoding(self):
        a = self.connection.execute(
            "select * from array(<a:int32 compression 'bitmap encoding'>[x=0:3,2,0], '[1,2][3,4]')")

        res = ''
        for pos, val in a:
            res += ('x:%d a:%s, ' % (pos['x'], val['a']))

        self.assertEqual(res, 'x:0 a:1, x:1 a:2, x:2 a:3, x:3 a:4, ')

    def test_compression_dictionary(self):
        a = self.connection.execute(
            "select * from array(<a:int32 compression 'dictionary'>[x=0:3,2,0], '[1,2][3,4]')")

        res = ''
        for pos, val in a:
            res += ('x:%d a:%s, ' % (pos['x'], val['a']))

        self.assertEqual(res, 'x:0 a:1, x:1 a:2, x:2 a:3, x:3 a:4, ')

    def test_compression_null_suppression(self):
        a = self.connection.execute(
            "select * from array(<a:int32 compression 'null suppression'>[x=0:3,2,0], '[1,2][3,4]')")

        res = ''
        for pos, val in a:
            res += ('x:%d a:%s, ' % (pos['x'], val['a']))

        self.assertEqual(res, 'x:0 a:1, x:1 a:2, x:2 a:3, x:3 a:4, ')

    def test_big_chunk(self):
        a = self.connection.execute(
            "select * from build(<a:int64>[x=0:1000,1000,0, y=0:50,50,0], x*y)")

        res = 0
        for pos, val in a:
            res += val['a']

        self.assertEqual(res, 638137500)

    @Cleanup("A")
    def test_dense_int8(self):
        r = self.connection.execute("create array A <x:int64> [a(int8)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:int8>[x=0:2,3,0], '[1,2,3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%d, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1, no:1 value:2, no:2 value:3, ")

    @Cleanup("A")
    def test_dense_uint8(self):
        r = self.connection.execute("create array A <x:int64> [a(uint8)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:uint8>[x=0:2,3,0], '[1,2,3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%d, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1, no:1 value:2, no:2 value:3, ")

    @Cleanup("A")
    def test_dense_int16(self):
        r = self.connection.execute("create array A <x:int64> [a(int16)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:int16>[x=0:2,3,0], '[1,2,3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%d, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1, no:1 value:2, no:2 value:3, ")

    @Cleanup("A")
    def test_dense_uint16(self):
        r = self.connection.execute("create array A <x:int64> [a(uint16)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:uint16>[x=0:2,3,0], '[1,2,3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%d, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1, no:1 value:2, no:2 value:3, ")

    @Cleanup("A")
    def test_dense_int32(self):
        r = self.connection.execute("create array A <x:int64> [a(int32)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:int32>[x=0:2,3,0], '[1,2,3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%d, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1, no:1 value:2, no:2 value:3, ")

    @Cleanup("A")
    def test_dense_uint32(self):
        r = self.connection.execute("create array A <x:int64> [a(uint32)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:uint32>[x=0:2,3,0], '[1,2,3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%d, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1, no:1 value:2, no:2 value:3, ")

    @Cleanup("A")
    def test_dense_uint64(self):
        r = self.connection.execute("create array A <x:int64> [a(uint64)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:uint64>[x=0:2,3,0], '[1,2,3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%d, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1, no:1 value:2, no:2 value:3, ")

    @Cleanup("A")
    def test_dense_float(self):
        r = self.connection.execute("create array A <x:int64> [a(float)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:float>[x=0:2,3,0], '[1.1,2.2,3.3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%.1f, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1.1, no:1 value:2.2, no:2 value:3.3, ")

    @Cleanup("A")
    def test_dense_double(self):
        r = self.connection.execute("create array A <x:int64> [a(double)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:double>[x=0:2,3,0], '[1.1,2.2,3.3]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%.1f, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:1.1, no:1 value:2.2, no:2 value:3.3, ")

    @Cleanup("A")
    def test_dense_char(self):
        r = self.connection.execute("create array A <x:int64> [a(char)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:char>[x=0:2,3,0], '[q,w,e]', True), A)", afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%s, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:e, no:1 value:q, no:2 value:w, ")

    @Cleanup("A")
    def test_dense_long_string(self):
        r = self.connection.execute("create array A <x:int64> [a(string)=3,3,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("redimension_store(build(<a:string>[x=0:2,3,0],"
                                "'[%s, %s, %s]', True), A)" % ("q" * 255, "w" * 500, "e" * 1000, ), afl=True)
        self.connection.complete()

        r = self.connection.execute("select * from A:a")
        res = ''
        for pos, val in r:
            res += 'no:%d value:%s, ' % (pos["no"], val['value'])
        self.connection.complete()
        self.assertEqual(res, "no:0 value:%s, no:1 value:%s, no:2 value:%s, " % ("e" * 1000, "q" * 255, "w" * 500, ))

    @Cleanup("A")
    def test_complete_cancel(self):
        r = self.connection.execute("create array A <a:string> [x=0:1,2,0]")
        self.assertEqual(r, None)
        self.connection.complete()

        self.connection.execute("select * into A from array(A, '[qwerty, asdfg]')")
        self.connection.cancel()

        r = self.connection.execute("select * from A")
        res = ''
        for pos, val in r:
            res += 'x:%d a:%s, ' % (pos["x"], val['a'])
        self.assertEqual(res, "")
        self.connection.complete()

        self.connection.execute("select * into A from array(A, '[qwerty, asdfg]')")
        self.connection.complete()

        r = self.connection.execute("select * from A")
        res = ''
        for pos, val in r:
            res += 'x:%d a:%s, ' % (pos["x"], val['a'])
        self.assertEqual(res, "x:0 a:qwerty, x:1 a:asdfg, ")
        self.connection.complete()

        self.connection.execute("select * into A from array(A, '[12345, 54321]')")
        self.connection.cancel()

        r = self.connection.execute("select * from A")
        res = ''
        for pos, val in r:
            res += 'x:%d a:%s, ' % (pos["x"], val['a'])
        self.assertEqual(res, "x:0 a:qwerty, x:1 a:asdfg, ")
        self.connection.complete()

        self.connection.execute("select * into A from array(A, '[12345, 54321]')")
        self.connection.complete()

        r = self.connection.execute("select * from A")
        res = ''
        for pos, val in r:
            res += 'x:%d a:%s, ' % (pos["x"], val['a'])
        self.assertEqual(res, "x:0 a:12345, x:1 a:54321, ")
        self.connection.complete()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(Basic)
    unittest.TextTestRunner(verbosity=2).run(suite)
