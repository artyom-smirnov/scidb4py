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

from bitstring import ConstBitStream
from types import *


class DenseChunk(object):
    def __init__(self, chunk_data, attribute, start_pos, end_pos, chunk_len, schema):
        self._chunk_data_stream = ConstBitStream(bytes=chunk_data)
        self._attribute_id = attribute.id
        self._start_pos = start_pos
        self._end_pos = end_pos
        self._chunk_len = chunk_len
        self._curr_elem = 0
        self._end = False
        self._schema = schema
        self._nullable = attribute.nullable
        self._buf_pos = 0

        self._n_elems = 1
        for i in range(0, len(self._start_pos)):
            interval = self._end_pos[i] - self._start_pos[i] + 1
            self._n_elems *= interval
        self._bitmap_size = ((self._n_elems + 7) >> 3) if attribute.nullable else 0

        self._varying_offs = 0
        self._elem_size = type_bitsize(attribute.type)
        if self._elem_size == 0:
            self._elem_size = 4
            self._varying_offs = self._n_elems * self._elem_size
        else:
            self._elem_size >>= 3

        self._calc_buf_pos()

        self._item_getter = {
            TID_INT8: self._get_int8,
            TID_INT16: self._get_int16,
            TID_INT32: self._get_int32,
            TID_INT64: self._get_int64,
            TID_UINT8: self._get_uint8,
            TID_UINT16: self._get_uint16,
            TID_UINT32: self._get_uint32,
            TID_UINT64: self._get_uint64,
            TID_FLOAT: self._get_float,
            TID_DOUBLE: self._get_double,
            TID_CHAR: self._get_char,
            TID_BOOL: self._get_bool,
            TID_STRING: self._get_string,
            TID_VOID: None
        }[attribute.type]

    def next_item(self):
        self._curr_elem += 1
        if self._curr_elem >= self._n_elems:
            self._end = True
            return
        self._calc_buf_pos()

    def _calc_buf_pos(self):
        self._buf_pos = self._bitmap_size + (
            self._curr_elem >> 3 if self._elem_size == 0 else self._curr_elem * self._elem_size)

    @property
    def end(self):
        return self._end

    @property
    def eof(self):
        return False

    def get_coordinates(self):
        l = self._curr_elem
        coords = {}
        for i in xrange(len(self._start_pos) - 1, -1, -1):
            pos = self._start_pos[i] + l % self._chunk_len[i]
            coords[self._schema.dimensions[i].name] = pos
            coords[i] = pos
            l /= self._chunk_len[i]
        return coords

    def get_item(self):
        if self.end:
            return None

        if self._nullable:
            null = False
            pos_tmp = self._buf_pos
            bitmap_pos = self._curr_elem >> 3
            self._chunk_data_stream.bytepos = bitmap_pos
            if self._chunk_data_stream.read('uintle:8') & (1 << (self._curr_elem & 7)):
                null = True
            self._buf_pos = pos_tmp
            if null:
                return None

        return self._item_getter()

    def _get_int8(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('intle:8')

    def _get_int16(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('intle:16')

    def _get_int32(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('intle:32')

    def _get_int64(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('intle:64')

    def _get_uint8(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('uintle:8')

    def _get_uint16(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('uintle:16')

    def _get_uint32(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('uintle:32')

    def _get_uint64(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('uintle:64')

    def _get_float(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('floatle:32')

    def _get_double(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return self._chunk_data_stream.read('floatle:64')

    def _get_char(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        return chr(self._chunk_data_stream.read('intle:8'))

    def _get_bool(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        b = self._chunk_data_stream.read('intle:8')
        return (b & (1 << (self._curr_elem & 7))) != 0

    def _get_string(self):
        self._chunk_data_stream.bytepos = self._buf_pos
        data_offset = self._chunk_data_stream.read('intle:32')
        _buf_pos = self._bitmap_size + data_offset + self._varying_offs
        self._chunk_data_stream.bytepos = _buf_pos

        s = self._chunk_data_stream.read('uintle:8')
        if s != 0:
            item_size = s
        else:
            (a, b, c, d) = self._chunk_data_stream.readlist('uintle:8, uintle:8, uintle:8, uintle:8')
            item_size = (a << 24) | (b << 16) | (c << 8) | d
        return str(self._chunk_data_stream.read('bytes:%d' % (item_size - 1)))
