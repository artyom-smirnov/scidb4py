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
from scidbpy.error import InternalError
from bitstring import ConstBitStream

RLE_PAYLOAD_MAGIC = 0xddddaaaa000eaaacL


class RLEChunkHeader(object):
    _fmt = 'uintle:64, intle:64, intle:64, intle:64, bool, pad:7'

    def __init__(self, bit_stream):
        (self._magic,
         self._elem_size,
         self._data_size,
         self._var_offs,
         self._is_boolean) = bit_stream.readlist(self._fmt)

        if self._magic != RLE_PAYLOAD_MAGIC:
            raise InternalError('Chunk payload is not RLE format')


class RLEChunkSegment(object):
    pass


class RLEChunk(object):
    def __init__(self, chunk_data, array_id, attribute_id, start_pos, end_pos, chunk_len, compression_method):
        self._chunk_data = ConstBitStream(bytes=chunk_data)
        self._array_id = array_id
        self._attribute_id = attribute_id
        self._start_pos = start_pos
        self._end_pos = end_pos
        self._chunk_len = chunk_len
        self._compression_method = compression_method
        self._chunk_header = RLEChunkHeader(self._chunk_data)

    @property
    def eof(self):
        return False

