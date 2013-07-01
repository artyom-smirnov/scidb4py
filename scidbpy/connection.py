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

import scidb_msg_pb2
from scidbpy.array import Array
from scidbpy._message import *
from scidbpy._network import Network
from scidbpy.result import Result


class Connection(object):
    def __init__(self, host='localhost', port=1239):
        """
        Constructor
        :param host: Host name or IP (default localhost)
        :param port: Port number (default 1239)
        """
        self._host = host
        self._port = port
        self._net = Network(host, port)
        self._result = None

    def open(self):
        """
        Open connection
        """
        self._net.open()

    def close(self):
        """
        Close connection
        """
        self._net.close()

    def execute(self, query_string, afl=False):
        r = scidb_msg_pb2.Query()
        r.query = query_string
        r.afl = afl

        h = Header(mtPrepareQuery, record_size=r.ByteSize())
        self._net.send(Message(h, r))

        msg = self._net.receive()
        self._query_id = msg.header.query_id

        r = scidb_msg_pb2.Query()
        r.query = ''
        r.afl = False

        h = Header(mtExecuteQuery, record_size=r.ByteSize(), query_id=self._query_id)
        self._net.send(Message(h, r))

        msg = self._net.receive()
        self._result = Result(msg)

        return Array(self._result, self._net) if self._result.selective else None

    def commit(self):
        """
        Commit query
        """
        h = Header(mtCompleteQuery, query_id=self._query_id)
        self._net.send(Message(h))
        self._net.receive()

    def rollback(self):
        """
        Rollback query
        """
        h = Header(mtCancelQuery, query_id=self._query_id)
        self._net.send(Message(h))
        self._net.receive()

    @property
    def result(self):
        return self._result
