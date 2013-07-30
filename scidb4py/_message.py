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
from struct import *

# Message IDs from SciDB sources (src/network/BaseConnection.h)
mtNone = 0
mtExecuteQuery = 1
mtPreparePhysicalPlan = 2
mtExecutePhysicalPlan = 3
mtFetch = 4
mtChunk = 5
mtChunkReplica = 6
mtRecoverChunk = 7
mtReplicaSyncRequest = 8
mtReplicaSyncResponse = 9
mtAggregateChunk = 10
mtQueryResult = 11
mtError = 12
mtSyncRequest = 13
mtSyncResponse = 14
mtCancelQuery = 15
mtRemoteChunk = 16
mtNotify = 17
mtWait = 18
mtBarrier = 19
mtMPISend = 20
mtAlive = 21
mtPrepareQuery = 22
mtResourcesFileExistsRequest = 23
mtResourcesFileExistsResponse = 24
mtAbort = 25
mtCommit = 26
mtCompleteQuery = 27
mtControl = 28
mtSystemMax = 29


class Header(object):
    _fmt = 'HHIIQQ'
    _headerSize = calcsize(_fmt)

    def __init__(self, message_type=0, record_size=0, binary_size=0, query_id=0):
        self._protocol_version = 3
        self._source_instance_id = ~0 & 0xFFFFFFFFFFFFFFFF  # equivalent of uint64_t sourceInstanceID = ~0
        self._message_type = message_type
        self._record_size = record_size
        self._binary_size = binary_size
        self._query_id = query_id

    def get_buf(self):
        return pack(self._fmt,
                    self._protocol_version,
                    self._message_type,
                    self._record_size,
                    self._binary_size,
                    self._source_instance_id,
                    self._query_id)

    def read_from_buf(self, buf):
        (
            self._protocol_version,
            self._message_type,
            self._record_size,
            self._binary_size,
            self._source_instance_id,
            self._query_id
        ) = unpack_from(self._fmt, buf)

    @property
    def protocol_version(self):
        return self._protocol_version

    @property
    def message_type(self):
        return self._message_type

    @property
    def source_instance_id(self):
        return self._source_instance_id

    @property
    def record_size(self):
        return self._record_size

    @property
    def binary_size(self):
        return self._binary_size

    @property
    def query_id(self):
        return self._query_id

    @classmethod
    def get_header_size(cls):
        return cls._headerSize


class Message(object):
    def __init__(self, header, record=None, binary=None):
        self._header = header
        self._record = record
        self._binary = binary

    @property
    def header(self):
        return self._header

    @property
    def record(self):
        return self._record

    @property
    def binary(self):
        return self._binary