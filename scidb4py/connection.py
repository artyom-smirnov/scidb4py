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

import _scidb_msg_pb2
from array import Array
from _message import *
from _network import Network
from result import Result
from error import *


QUERY_AUTO_NONE     = 0
QUERY_AUTO_COMPLETE = 1
QUERY_AUTO_CANCEL   = 2


class Connection(object):
    def __init__(self, host='localhost', port=1239, auto=QUERY_AUTO_COMPLETE):
        """
        Constructor
        :param host: Host name or IP (default localhost)
        :param port: Port number (default 1239)
        """
        self._host = host
        self._port = port
        self._net = Network(host, port)
        self._result = None
        self._query_id = -1
        self._auto = auto

    def open(self):
        """
        Open connection
        """
        self._net.open()

    def close(self):
        """
        Close connection
        """
        self._complete_or_cancel()
        self._net.close()

    def execute(self, query_string, afl=False):
        self._complete_or_cancel()
        r = _scidb_msg_pb2.Query()
        #noinspection PyUnresolvedReferences
        r.query = query_string
        #noinspection PyUnresolvedReferences
        r.afl = afl

        h = Header(mtPrepareQuery, record_size=r.ByteSize())
        self._net.send(Message(h, r))

        msg = self._net.receive()
        self._query_id = msg.header.query_id

        r = _scidb_msg_pb2.Query()
        #noinspection PyUnresolvedReferences
        r.query = ''
        #noinspection PyUnresolvedReferences
        r.afl = False

        h = Header(mtExecuteQuery, record_size=r.ByteSize(), query_id=self._query_id)
        self._net.send(Message(h, r))

        msg = self._net.receive()
        self._result = Result(msg)

        return Array(self._query_id, self._result.schema, self._net) if self._result.selective else None

    def complete(self):
        """
        Commit query
        """
        if not self.active:
            raise InternalError('No active query to complete')
        h = Header(mtCompleteQuery, query_id=self._query_id)
        self._net.send(Message(h))
        self._net.receive()
        self._query_id = -1
        self._result = None

    def cancel(self):
        """
        Rollback query
        """
        if not self.active:
            raise InternalError('No active query to cancel')
        h = Header(mtCancelQuery, query_id=self._query_id)
        self._net.send(Message(h))
        self._net.receive()
        self._query_id = -1
        self._result = None

    @property
    def active(self):
        return self._query_id != -1

    @property
    def result(self):
        return self._result

    def _complete_or_cancel(self):
        if not self.active:
            return
        if self._auto == QUERY_AUTO_COMPLETE:
            self.complete()
        elif self._auto == QUERY_AUTO_CANCEL:
            self.cancel()
