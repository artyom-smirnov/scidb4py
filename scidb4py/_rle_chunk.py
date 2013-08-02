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
from error import InternalError

RLE_PAYLOAD_MAGIC = 0xddddaaaa000eaaacL


class RLEChunkHeader(object):
    _fmt = 'uintle:64, intle:64, intle:64, intle:64, intle:64, intle:8, pad:56'

    def __init__(self, bit_stream):
        (self._magic,
         self._n_segs,
         self._elem_size,
         self._data_size,
         self._var_offs,
         self._is_boolean) = bit_stream.readlist(self._fmt)

        self._is_boolean = bool(self._is_boolean)

        if self._magic != RLE_PAYLOAD_MAGIC:
            raise InternalError('Chunk payload is not RLE format')

    @property
    def n_segs(self):
        return self._n_segs

    @property
    def elem_size(self):
        return self._elem_size

    @property
    def data_size(self):
        return self._data_size

    @property
    def var_offs(self):
        return self._var_offs

    @property
    def is_boolean(self):
        return self._is_boolean


class RLEChunkSegment(object):
    _fmt = 'intle:64, intle:32'

    def __init__(self, bit_stream):
        (
            self._p_position,
            i
        ) = bit_stream.readlist(self._fmt)
        self._value_index = i & 0x3fffffff
        self._same = (i & 0x40000000) != 0
        self._is_null = (i & 0x80000000) != 0

    @property
    def p_position(self):
        return self._p_position

    @property
    def value_index(self):
        return self._value_index

    @property
    def same(self):
        return self._same

    @property
    def is_null(self):
        return self._is_null


class RLEChunk(object):
    def __init__(self, chunk_data, attribute, start_pos, end_pos, chunk_len, schema):
        self._chunk_data_stream = ConstBitStream(bytes=chunk_data)
        self._attribute_id = attribute.id
        self._type_id = attribute.type
        self._start_pos = start_pos
        self._end_pos = end_pos
        self._chunk_len = chunk_len
        self._chunk_header = RLEChunkHeader(self._chunk_data_stream)
        self._schema = schema

        self._cur_seg = 0
        self._cur_value_index = 0
        self._cur_item_in_seg = 0
        self._element_number = 0
        self._segments = []

        self._end = self._chunk_header.n_segs == 0
        for i in range(0, self._chunk_header.n_segs + 1):
            self._segments.append(RLEChunkSegment(self._chunk_data_stream))

        self._payload_start = self._chunk_data_stream.bytepos

        self._gets = {
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
        }

        self._eval_cur_value_index()

    @property
    def start_pos(self):
        return self._start_pos

    @property
    def end_pos(self):
        return self._end_pos

    @property
    def len(self):
        return self._chunk_len

    @property
    def eof(self):
        return False

    @property
    def end(self):
        return self._end

    @property
    def type(self):
        return self._type_id

    def next_item(self):
        if self.end or self._cur_seg == len(self._segments) - 1:
            return False

        seg = self._segments[self._cur_seg]
        size = self._segments[self._cur_seg + 1].p_position - seg.p_position
        while True:
            self._cur_item_in_seg += 1
            if self._cur_item_in_seg < size:
                self._element_number += 1
                self._eval_cur_value_index()
                return True
            else:
                self._cur_seg += 1
                self._cur_item_in_seg = -1
                if self._cur_seg == len(self._segments) - 1:
                    self._end = True
                    return False
                seg = self._segments[self._cur_seg]
                size = self._segments[self._cur_seg + 1].p_position - seg.p_position

    def get_item(self):
        if self._segments[self._cur_seg].is_null:
            return None

        return self._gets[self.type]()

    def get_coordinates(self):
        l = self._element_number
        coords = {}
        for i in xrange(len(self._start_pos) - 1, -1, -1):
            pos = self._start_pos[i] + l % self._chunk_len[i]
            coords[self._schema.dimensions[i].name] = pos
            coords[i] = pos
            l /= self._chunk_len[i]
        return coords

    def _get_int8(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('intle:8')

    def _get_int16(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('intle:16')

    def _get_int32(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('intle:32')

    def _get_int64(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('intle:64')

    def _get_uint8(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('uintle:8')

    def _get_uint16(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('uintle:16')

    def _get_uint32(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('uintle:32')

    def _get_uint64(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('uintle:64')

    def _get_float(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('floatle:32')

    def _get_double(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return self._chunk_data_stream.read('floatle:64')

    def _get_char(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        return chr(self._chunk_data_stream.read('intle:8'))

    def _get_bool(self):
        p = self._cur_value_index - self._payload_start
        self._chunk_data_stream.bytepos = self._payload_start + (p >> 3)
        b = self._chunk_data_stream.read('intle:8')
        return (b & (1 << (p & 7))) != 0

    def _get_string(self):
        self._chunk_data_stream.bytepos = self._cur_value_index
        offset = self._chunk_data_stream.read('intle:32')
        self._chunk_data_stream.bytepos = self._payload_start + self._chunk_header.var_offs + offset
        b = self._chunk_data_stream.read('uintle:8')
        if b == 0:
            size = self._chunk_data_stream.read('intle:32')
        else:
            size = b

        chars = self._chunk_data_stream.read('bytes:%d' % (size - 1))
        return str(chars)

    def _eval_cur_value_index(self):
        if self.end:
            return

        seg = self._segments[self._cur_seg]
        if seg.is_null:
            return

        size = 4 if self._chunk_header.elem_size == 0 else self._chunk_header.elem_size

        if seg.same:
            self._cur_value_index = self._payload_start + seg.value_index * size
        else:
            self._cur_value_index = self._payload_start + (seg.value_index + self._cur_item_in_seg) * size
