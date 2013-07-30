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
from schema import *


class Result(object):
    def __init__(self, query_result_msg):
        """
        :param query_result_msg: Query result network message
        """
        hdr = query_result_msg.header
        rec = query_result_msg.record
        array_name = rec.array_name

        self._schema = None
        if len(rec.attributes) > 0:
            attributes = []
            for a in rec.attributes:
                attributes.append(Attribute(a.id, a.name, a.type, a.flags))
            dimensions = []
            for d in rec.dimensions:
                dimensions.append(
                    Dimension(
                        d.name,
                        d.type_id,
                        d.flags,
                        d.start_min,
                        d.end_max,
                        d.chunk_interval,
                        d.mapping_array_name,
                        d.coordinates_mapping_size,
                        d.coordinates_mapping
                    )
                )
            self._schema = Schema(array_name, attributes, dimensions)

        self._query_id = hdr.query_id
        self._selective = rec.selective
        self._explain_logical = rec.explain_logical
        self._explain_physical = rec.explain_physical

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

    @property
    def selective(self):
        return self._selective

    @property
    def explain_logical(self):
        return self._explain_logical

    @property
    def explain_physical(self):
        return self._explain_physical
