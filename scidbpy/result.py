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
from scidbpy.schema import *


class Result(object):
    def __init__(self, query_result_msg):
        """
        :param query_result_msg: Query result network message
        """
        hdr = query_result_msg.header
        rec = query_result_msg.record
        array_name = rec.array_name
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
                    d.curr_start,
                    d.curr_end,
                    d.end_max,
                    d.chunk_interval
                )
            )
        self._query_id = hdr.query_id
        self._schema = Schema(array_name, attributes, dimensions)

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