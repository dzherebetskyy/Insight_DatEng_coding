# Insight_DatEng_coding
Written by Danylo Zherebetskyy in Python on July 11,2016
for Insight Data Engineering - Coding Challenge

Written code:  rolling_median.py

New zip-master with passed predesigned tests and own created tests: coding-challenge-master_DZ.zip
[Mon Jul 11 14:58:30 PDT 2016] 1 of 1 tests passed

Requires to 
import sys, os, json, datetime, numpy

Works with data up to 1GB

Cons:  -code is not dynamic - does not work with the constantly updating input
       -for large data >1GB may require a lot of RAM

Possible solution: write using iterative IJSON 

