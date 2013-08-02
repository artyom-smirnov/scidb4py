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

from distutils.core import setup
from distutils.spawn import find_executable
import os
import sys
import subprocess

protoc = find_executable("protoc")
protoc_in = 'scidb4py/_scidb_msg.proto'
protoc_out = 'scidb4py/_scidb_msg_pb2.py'

if (not os.path.exists(protoc_out) or
        (os.path.getmtime(protoc_in) > os.path.getmtime(protoc_out))):
    print "Generating %s..." % protoc_out
    if protoc is None:
        sys.stderr.write(
            "##################################################\n"
            "# 'protoc' binary not found in PATH!             #\n"
            "# You need 'protoc' for compiling protobuf file: #\n"
            "# https://code.google.com/p/protobuf/            #\n"
            "##################################################\n")
        sys.exit(1)

    protoc_command = [protoc, "-I.", "--python_out=.", protoc_in]
    if subprocess.call(protoc_command) != 0:
        sys.exit(1)

from scidb4py import __version__

setup(name='scidb4py',
      version=__version__,
      description='Pure python SciDB client library implementation',
      long_description=open('README.rst').read() + '\n' + open('CHANGELOG.rst').read(),
      url='https://github.com/artyom-smirnov/scidb4py',
      author='Artyom Smirnov',
      author_email='artyom_smirnov@icloud.com',
      license='GPLv3',
      packages=['scidb4py'],
      platforms=['any'],
      requires=['protobuf', 'bitstring'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Topic :: Software Development :: Libraries',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX',
          'Programming Language :: Python'
      ]
)
