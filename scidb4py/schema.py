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


class Attribute(object):
    def __init__(self, att_id, name, type_id, flags):
        """

        :param att_id: Attribute ID
        :param name: Attribute name
        :param flags: Attribute flags 1 - nullable, 2 - empty indicator
        :param type_id: Attribute type
        """
        self._id = att_id
        self._name = name
        self._type_id = type_id
        self._nullable = flags & 1
        self._empty_indicator = flags & 2

    @property
    def id(self):
        """
        Attribute ID

        :rtype : int
        :return: attribute ID
        """
        return self._id

    @property
    def name(self):
        """
        Attribute name

        :rtype : str
        :return: attribute name
        """
        return self._name

    @property
    def type(self):
        """
        Attribute type

        :rtype : str
        :return: attribute type
        """
        return self._type_id

    @property
    def nullable(self):
        return self._nullable

    @property
    def empty_indicator(self):
        return self._empty_indicator

    def __str__(self):
        return self.name + ':' + self.type


class Dimension(object):
    def __init__(self, name, type_id, flags, start, end, chunk_interval,
                 mapping_array_name=None, coordinates_mapping_size=0, coordinates_mapping=None):
        """
        :param name: Dimension name
        :param type_id: Dimension type
        """
        self._name = name
        self._type_id = type_id
        self._flags = flags
        self._start = start
        self._end = end
        self._chunk_interval = chunk_interval
        self._mapping_array_name = mapping_array_name
        self._coordinates_mapping_size = coordinates_mapping_size
        self._coordinates_mapping = coordinates_mapping

    @property
    def name(self):
        """
        Dimension name

        :rtype : str
        :return: dimension name
        """
        return self._name

    @property
    def type(self):
        """
        Dimension type

        :rtype : str
        :return: dimension type
        """
        return self._type_id

    @property
    def flags(self):
        return self._flags

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    @property
    def chunk_interval(self):
        return self._chunk_interval

    @property
    def mapping_array_name(self):
        return self._mapping_array_name

    @property
    def coordinates_mapping_size(self):
        return self._coordinates_mapping_size

    @property
    def coordinates_mapping(self):
        return self._coordinates_mapping

    def __str__(self):
        return self.name + '(' + self.type + ')'


class Schema(object):
    def __init__(self, array_name, attributes, dimensions):
        """
        :param array_name: array name
        :param attributes:
        :param dimensions:
        """
        self._array_name = array_name
        self._attributes = attributes
        self._dimensions = dimensions

    @property
    def array_name(self):
        """
        Array name

        :rtype : str
        :return: array name
        """
        return self._array_name

    @property
    def attributes(self):
        """
        Attributes list

        :rtype : list
        :return: attributes
        """
        return self._attributes

    @property
    def dimensions(self):
        """
        Dimensions list

        :rtype : list
        :return: dimensions
        """
        return self._dimensions

    def __str__(self):
        attrs = ', '.join(str(x) for x in self._attributes)
        dims = ', '.join(str(x) for x in self._dimensions)
        return self._array_name + '<' + attrs  + '>[' + dims + ']'