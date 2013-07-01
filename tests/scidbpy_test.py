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
from scidbpy import Connection


class Basic(unittest.TestCase):
    connection = None

    @classmethod
    def setUpClass(cls):
        cls.connection = Connection('10.81.1.145', 1239)
        cls.connection.open()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def test_select(self):
        self.connection.prepare("select * from array(empty <a:string null>[x=0:2,3,0], '[\"qwe\",(),\"eeeee\"]')")
        a = self.connection.execute()

        while True:
            a.fetch()
            if a.eof:
                break
            c = a.get_chunk(1)

            while True:
                if c.end:
                    break
                print c.get_coordinates()
                print c.next()

if __name__ == '__main__':
    unittest.main()
