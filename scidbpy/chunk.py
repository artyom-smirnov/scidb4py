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
from scidbpy.rle_chunk import RLEChunk, RLE_PAYLOAD_MAGIC
from scidbpy.rle_bitmap_chunk import RLEBitmapChunk, RLE_BITMAP_PAYLOAD_MAGIC


class DummyEOFChunk(object):
    @property
    def eof(self):
        return True


def make_chunk(chunk_msg, array):
    rec = chunk_msg.record

    if rec.eof:
        return DummyEOFChunk()

    array_id = rec.array_id
    attribute_id = rec.attribute_id
    attribute = array.schema.attributes[attribute_id]
    sparse = rec.sparse
    compression_method = rec.compression_method
    chunk_data = chunk_msg.binary
    rle = rec.rle

    if sparse:
        raise NotImplementedError('Sparse chunks not supported yet')

    if compression_method != 0:
        raise NotImplementedError('Compressed chunks not supported yet')

    start_pos = []
    end_pos = []
    chunk_len = []
    for i, coord in enumerate(rec.coordinates):
        dim = array.schema.dimensions[i]
        end_coord = coord + dim.chunk_interval + 1
        end_coord = dim.end_max if end_coord > dim.end_max else end_coord
        start_pos.append(coord)
        end_pos.append(end_coord)
        chunk_len.append(end_coord - coord + 1)

    magic = ConstBitStream(bytes=chunk_data, length=64).read('uintle:64')
    if rle:
        if magic == RLE_PAYLOAD_MAGIC:
            return RLEChunk(chunk_data, array_id, attribute, start_pos, end_pos, chunk_len)
        elif magic == RLE_BITMAP_PAYLOAD_MAGIC:
            return RLEBitmapChunk(chunk_data, array_id, attribute, start_pos, end_pos, chunk_len)
        else:
            raise InternalError('Unknown chunk format')
    else:
        raise InternalError('Dense chunk not yet supported')

