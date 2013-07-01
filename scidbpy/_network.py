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

import socket

import scidb_msg_pb2
from scidbpy._message import *
from scidbpy.error import *


class Network(object):
    def __init__(self, host, port):
        """
        :param host: Host name or IP (default localhost)
        :param port: Port number (default 1239)
        """
        self._host = host
        self._port = port

    def open(self):
        self._socket = socket.create_connection((self._host, self._port))

    def close(self):
        self._socket.close()

    def send(self, message):
        self._socket.send(message.header.get_buf())
        rec = message.record
        if rec:
            self._socket.send(rec.SerializeToString())
        binary = message.binary
        if binary:
            self._socket.send(binary)

    def receive(self):
        h = Header()
        h.read_from_buf(self._socket.recv(Header.get_header_size()))

        rec = None
        if h.record_size > 0:
            recBuf = self._socket.recv(h.record_size)
            if h.message_type == mtError:
                rec = scidb_msg_pb2.Error()
                rec.ParseFromString(recBuf)
                raise ExecutionError(rec.what_str)
            elif h.message_type == mtQueryResult:
                rec = scidb_msg_pb2.QueryResult()
            elif h.message_type == mtChunk:
                rec = scidb_msg_pb2.Chunk()
            else:
                raise InternalError('Unknown network message %d' % h.message_type)
            rec.ParseFromString(recBuf)

        binBuf = None
        if h.binary_size > 0:
            binBuf = self._socket.recv(h.binary_size)

        return Message(h, rec, binBuf)

