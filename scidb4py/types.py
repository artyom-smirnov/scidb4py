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

TID_INDICATOR   = "indicator"
TID_CHAR        = "char"
TID_INT8        = "int8"
TID_INT16       = "int16"
TID_INT32       = "int32"
TID_INT64       = "int64"
TID_UINT8       = "uint8"
TID_UINT16      = "uint16"
TID_UINT32      = "uint32"
TID_UINT64      = "uint64"
TID_FLOAT       = "float"
TID_DOUBLE      = "double"
TID_BOOL        = "bool"
TID_STRING      = "string"
TID_DATETIME    = "datetime"
TID_DATETIMEZ   = "datetimetz"
TID_BINARY      = "binary"
TID_VOID        = "void"

BUILTIN_TYPES = {
    TID_INDICATOR:    1,
    TID_CHAR:         8,
    TID_INT8:         8,
    TID_INT16:        16,
    TID_INT32:        32,
    TID_INT64:        64,
    TID_UINT8:        8,
    TID_UINT16:       16,
    TID_UINT32:       32,
    TID_UINT64:       64,
    TID_FLOAT:        32,
    TID_DOUBLE:       64,
    TID_BOOL:         1,
    TID_STRING:       0,
    TID_DATETIME:     64,
    TID_DATETIMEZ:    128,
    TID_BINARY:       0,
    TID_VOID:         0
}


def is_scidb_type(type_id):
    return BUILTIN_TYPES.has_key(type_id)


def type_bitsize(type_id):
    return BUILTIN_TYPES[type_id]
