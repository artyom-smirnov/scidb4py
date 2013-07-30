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
from error import InternalError
from bitstring import ConstBitStream

RLE_BITMAP_PAYLOAD_MAGIC = 0xeeeeaaaa00eebaacL


class RLEBitmapChunkHeader(object):
    _fmt = 'uintle:64, intle:64, intle:64'

    def __init__(self, bit_stream):
        (
            self._magic,
            self._n_segs,
            self._n_non_empty_elements
        ) = bit_stream.readlist(self._fmt)

        if self._magic != RLE_BITMAP_PAYLOAD_MAGIC:
            raise InternalError('Chunk payload is not RLE-bitmap format')

    @property
    def n_segs(self):
        return self._n_segs

    @property
    def n_non_empty_elements(self):
        return self._n_non_empty_elements


class RLEBitmapChunkSegment(object):
    _fmt = 'intle:64, intle:64, intle:64'

    def __init__(self, bit_stream):
        (
            self._l_position,
            self._length,
            self._p_position
        ) = bit_stream.readlist(self._fmt)

    @property
    def l_position(self):
        return self._l_position

    @property
    def length(self):
        return self._length

    @property
    def p_position(self):
        return self._p_position


class RLEBitmapChunk(object):
    def __init__(self, chunk_data, attribute, start_pos, end_pos, chunk_len, schema):
        self._chunk_data_stream = ConstBitStream(bytes=chunk_data)
        self._attribute_id = attribute.id
        self._start_pos = start_pos
        self._end_pos = end_pos
        self._chunk_len = chunk_len
        self._schema = schema

        self._chunk_header = RLEBitmapChunkHeader(self._chunk_data_stream)

        self._segments = []

        self._end = self._chunk_header.n_segs == 0
        for i in range(0, self._chunk_header.n_segs):
            self._segments.append(RLEBitmapChunkSegment(self._chunk_data_stream))

        self._cur_logical_position = 0
        self._n_non_empty_items = 0
        self._cur_seg = 0

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

    def next_item(self):
        if self.end:
            return False

        self._cur_logical_position += 1
        self._n_non_empty_items += 1

        if self._cur_logical_position >= (self._segments[self._cur_seg].l_position + self._segments[self._cur_seg].length):
            self._cur_seg += 1
            if self._cur_seg == len(self._segments) or (self._n_non_empty_items > self._chunk_header.n_non_empty_elements):
                self._end = True
                return False
            self._cur_logical_position = self._segments[self._cur_seg].l_position
        return True

    def get_coordinates(self):
        l = self._cur_logical_position
        coords = {}
        for i in xrange(len(self._start_pos)-1, -1, -1):
            pos = self._start_pos[i] + l % self._chunk_len[i]
            coords[self._schema.dimensions[i].name] = pos
            coords[i] = pos
            l /= self._chunk_len[i]
        return coords
