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

scidb_host = 'localhost'


class Basic(unittest.TestCase):
    connection = None

    @classmethod
    def setUpClass(cls):
        cls.connection = Connection(scidb_host)
        cls.connection.open()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def test_select(self):
        a = self.connection.execute("select * from array(<a:int32 null>[x=0:2,3,0, y=0:2,3,0], '[[1,2,3][4,5,6][7,8,9]]')")
        print self.connection.result.selective

        while not a.end:
            print '%s - %s' % (a.get_coordinates(), a.get_item("a"))
            a.next_item(True)

if __name__ == '__main__':
    scidb_host = os.getenv('SCIDB_HOST', scidb_host)
    unittest.main()
