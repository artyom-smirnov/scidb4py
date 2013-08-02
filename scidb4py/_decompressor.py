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
import zlib
import bz2

# From include/array/Compressor.h
NO_COMPRESSION = 0
NULL_FILTER = 1
RUN_LENGTH_ENCODING = 2
BITMAP_ENCODING = 3
NULL_SUPPRESSION = 4
DICTIONARY_ENCODING = 5
ZLIB = 6
BZLIB = 7


def decompress(compression_method, buf):
    # SciDB do not use format sensitive compressors so just ignore it
    if compression_method in (NULL_FILTER, RUN_LENGTH_ENCODING, BITMAP_ENCODING, NULL_SUPPRESSION, DICTIONARY_ENCODING):
        return buf
    elif compression_method == ZLIB:
        return zlib.decompress(buf)
    elif compression_method == BZLIB:
        return bz2.decompress(buf)
    else:
        raise NotImplementedError('Unknown or unsupported compression type: %d' % compression_method)
