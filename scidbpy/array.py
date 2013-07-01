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
from scidbpy.chunk import make_chunk
from scidbpy._message import Header, mtFetch, Message


class Array(object):
    def __init__(self, result, network):
        self._query_id = result.query_id
        self._schema = result.schema
        self._net = network
        self._eof = False
        self._next_chunks = None
        self._chunks = []

    def fetch(self):
        """
        Fetch new chunks for each attribute

        :rtype : list
        :return: chunks list
        """
        self._chunks = []
        for a in self.schema.attributes:
            r = scidb_msg_pb2.Fetch()
            r.attribute_id = a.id
            r.array_name = self.schema.array_name

            h = Header(mtFetch, record_size=r.ByteSize(), query_id=self._query_id)
            self._net.send(Message(h, r))

            msg = self._net.receive()
            chunk = make_chunk(msg, self)
            self._eof |= chunk.eof

            self._chunks.append(chunk)

        return self._chunks

    @property
    def eof(self):
        return self._eof

    def get_chunk(self, attributeID):
        """
        Get chunk by attribute id
        :param attributeID: attribute id
        :rtype : scidbpy.rle_chunk.RLEChunk
        :return: chunk
        """
        return self._chunks[attributeID]

    @property
    def query_id(self):
        """
        Query ID

        :rtype : int
        :return: query ID
        """
        return self._query_id

    @property
    def schema(self):
        """
        Array schema

        :rtype : scidbpy.schema.Schema
        :return: array schema
        """
        return self._schema