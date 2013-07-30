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
from _chunk import make_chunk
from _message import Header, mtFetch, Message
from types import *
from schema import *


class Array(object):
    def __init__(self, query_id, schema, network):
        self._query_id = query_id
        self._schema = schema
        self._net = network
        self._array_end = False
        self._chunks = []
        self._bitmap = None
        self._chunk_end = False

        self._attributes_name_id_mapping = {}
        for a in self.schema.attributes:
            self._attributes_name_id_mapping[a.name] = a.id

        self._nid_mapping = {}
        for d in self.schema.dimensions:
            if d.type != TID_INT64:
                if is_scidb_type(d.type):
                    label_attr = 'value'
                    attributes = [
                        Attribute(0, 'value', d.type, 0),
                    ]
                else:
                    label_attr = 'label'
                    attributes = [
                        Attribute(0, 'value', TID_VOID, 0),
                        Attribute(1, 'label', TID_STRING, 0)
                    ]

                dimensions = [Dimension('no', TID_INT64, 0, d.start, d.start + d.coordinates_mapping_size - 1, d.coordinates_mapping_size)]
                mapping_array = Array(query_id,
                                      Schema(d.mapping_array_name,
                                             attributes,
                                             dimensions
                                             ),
                                      network)
                mapping = []
                for dim, att in mapping_array:
                    mapping.append({dim['no']: att[label_attr]})
                self._nid_mapping[d.name] = mapping

        self.next_chunk()

    def nid_mapping(self, dimension):
        return self._nid_mapping[dimension]

    def next_chunk(self):
        """
        Fetch new chunks for each attribute

        :rtype : list
        :return: chunks list
        """
        self._chunk_end = False
        self._chunks = []
        for a in self.schema.attributes:
            r = _scidb_msg_pb2.Fetch()
            #noinspection PyUnresolvedReferences
            r.attribute_id = a.id
            #noinspection PyUnresolvedReferences
            r.array_name = self.schema.array_name

            h = Header(mtFetch, record_size=r.ByteSize(), query_id=self._query_id)
            self._net.send(Message(h, r))

            msg = self._net.receive()
            chunk = make_chunk(msg, self)

            self._array_end |= chunk.eof
            self._chunk_end |= chunk.end

            if a.type == TID_INDICATOR:
                self._bitmap = chunk
            else:
                self._chunks.append(chunk)

        return self._chunks

    def next_item(self):
        if self._bitmap is not None:
            self._bitmap.next_item()
            self._chunk_end |= self._bitmap.end
        for c in self._chunks:
            c.next_item()
            self._chunk_end |= c.end

    def __iter__(self):
        while True:
            if self.end:
                return

            if self.chunk_end:
                self.next_chunk()
                continue

            attributes = {}
            for a in self.schema.attributes:
                if a.type == TID_INDICATOR:
                    continue
                val = self.get_item(a.id)
                attributes[a.id] = val
                attributes[a.name] = val

            yield (self.get_coordinates(), attributes)
            self.next_item()

    def get_coordinates(self):
        if self.bitmap is None:
            return self._chunks[0].get_coordinates()
        else:
            return self.bitmap.get_coordinates()

    @property
    def chunk_end(self):
        return self._chunk_end

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
    def end(self):
        return self._array_end

    def get_chunk(self, attribute_id):
        """
        Get chunk by attribute id
        :param attribute_id: attribute id
        :rtype : scidb4py.rle_chunk.RLEChunk
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