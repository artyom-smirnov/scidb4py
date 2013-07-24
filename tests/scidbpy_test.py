"""
This file is part of scidbpy.  scidbpy is free software: you can
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
from scidbpy import Connection
from scidbpy.types import *

scidb_host = os.getenv('SCIDB_HOST', 'localhost')


class Basic(unittest.TestCase):
    connection = None

    #noinspection PyBroadException
    @classmethod
    def setUpClass(cls):
        cls.connection = Connection(scidb_host)
        cls.connection.open()

        try:
            cls.connection.execute("drop array A")
            cls.connection.complete()
        except:
            pass

        try:
            cls.connection.execute("drop array B")
            cls.connection.complete()
        except:
            pass

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

    def test_non_selective(self):
        a = self.connection.execute("create array A <a:int32 null> [x=0:2,3,0, y=0:2,3,0]")
        self.connection.complete()
        self.assertEqual(a, None)
        self.assertFalse(self.connection.result.selective)

        a = self.connection.execute("select * from array(A, '[[1,2,3][4,5,6][7,8,9]]')")
        self.connection.complete()
        self.assertNotEqual(a, None)
        self.assertTrue(self.connection.result.selective)

        a = self.connection.execute("drop array A")
        self.connection.complete()
        self.assertEqual(a, None)
        self.assertFalse(self.connection.result.selective)

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

    def test_char(self):
        a = self.connection.execute(
            "select * from array(<a:char, b:char null>[x=0:3,2,0], '[(a, b)()][(c, d)(e,null)]')")

        res = ''
        for dim, att in a:
            res += ('x:%d a:%s b:%s, ' % (dim['x'], att['a'], att['b']))

        self.assertEqual(res, 'x:0 a:a b:b, x:2 a:c b:d, x:3 a:e b:None, ')

    def test_mapping_arrays(self):
        r = self.connection.execute("create array A <x:int64> [a(string)=3,3,0]")
        self.connection.complete()
        self.assertEqual(r, None)
        self.assertFalse(self.connection.result.selective)

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

        r = self.connection.execute("drop array A")
        self.connection.complete()
        self.assertEqual(r, None)
        self.assertFalse(self.connection.result.selective)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(Basic)
    unittest.TextTestRunner(verbosity=2).run(suite)
