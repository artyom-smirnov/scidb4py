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
from types import *


class Array(object):
    def __init__(self, result, network):
        self._query_id = result.query_id
        self._schema = result.schema
        self._net = network
        self._eof = False
        self._chunks = []
        self._bitmap = None
        self._end = False

        self._attributes_name_id_mapping = {}
        for a in self.schema.attributes:
            self._attributes_name_id_mapping[a.name] = a.id

        self.next_chunk()

    def next_chunk(self):
        """
        Fetch new chunks for each attribute

        :rtype : list
        :return: chunks list
        """
        self._end = False
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
            self._end |= chunk.end

            if a.type == TID_INDICATOR:
                self._bitmap = chunk
            else:
                self._chunks.append(chunk)

        return self._chunks

    def next_item(self, next_chunk=False):
        if self.eof or self.end:
            return

        if self._bitmap is not None:
            self._bitmap.next_item()
            self._end |= self._bitmap.end
        for c in self._chunks:
            c.next_item()
            self._end |= c.end

        if self.end and next_chunk:
            self.next_chunk()

    def get_coordinates(self):
        if self.bitmap is None:
            return self._chunks[0].get_coordinates()
        else:
            return self.bitmap.get_coordinates()

    @property
    def end(self):
        return self._end

    def get_item(self, attribute_id):
        if isinstance(attribute_id, int):
            return self._chunks[attribute_id].get_item()
        elif isinstance(attribute_id, (str, unicode, basestring)):
            return self._chunks[self._attributes_name_id_mapping[attribute_id]].get_item()
        else:
            raise TypeError("Integer or string expected")

    @property
    def bitmap(self):
        return self._bitmap

    @property
    def eof(self):
        return self._eof

    def get_chunk(self, attribute_id):
        """
        Get chunk by attribute id
        :param attribute_id: attribute id
        :rtype : scidbpy.rle_chunk.RLEChunk
        :return: chunk
        """
        if isinstance(attribute_id, int):
            return self._chunks[attribute_id]
        elif isinstance(attribute_id, (str, unicode, basestring)):
            return self._chunks[self._attributes_name_id_mapping[attribute_id]]
        else:
            raise TypeError("Integer or string expected")

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