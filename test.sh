#!/bin/bash

PYTHONPATH=`pwd` coverage run tests/scidbpy_test.py
coverage report
coverage html

