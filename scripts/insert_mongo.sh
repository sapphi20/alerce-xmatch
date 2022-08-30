#!/bin/bash

for f in /data/raw2/*;do
	echo ${f}
	python3 tbl_to_mongo.py ${f}
done
