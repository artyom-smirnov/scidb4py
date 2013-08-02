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

import socket

import _scidb_msg_pb2
from _message import *
from error import *


class Network(object):
    def __init__(self, host, port):
        """
        :param host: Host name or IP (default localhost)
        :param port: Port number (default 1239)
        """
        self._host = host
        self._port = port
        self._socket = None

    def open(self):
        self._socket = socket.create_connection((self._host, self._port))

    def close(self):
        self._socket.close()

    def send(self, message):
        self._socket.sendall(message.header.get_buf())
        rec = message.record
        if rec:
            self._socket.sendall(rec.SerializeToString())
        binary = message.binary
        if binary:
            self._socket.sendall(binary)

    def receive(self):
        h = Header()
        h.read_from_buf(self._socket.recv(Header.get_header_size(), socket.MSG_WAITALL))

        rec = None
        if h.record_size > 0:
            recBuf = self._socket.recv(h.record_size, socket.MSG_WAITALL)
            if h.message_type == mtError:
                rec = _scidb_msg_pb2.Error()
                rec.ParseFromString(recBuf)
                if rec.long_error_code == 0:
                    return None
                raise ExecutionError(rec.what_str)
            elif h.message_type == mtQueryResult:
                rec = _scidb_msg_pb2.QueryResult()
            elif h.message_type == mtChunk:
                rec = _scidb_msg_pb2.Chunk()
            else:
                raise InternalError('Unknown network message %d' % h.message_type)
            rec.ParseFromString(recBuf)

        binBuf = None
        if h.binary_size > 0:
            binBuf = self._socket.recv(h.binary_size, socket.MSG_WAITALL)

        return Message(h, rec, binBuf)

