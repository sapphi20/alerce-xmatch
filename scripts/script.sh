#!/bin/bash

for f in /data/raw2/*.tbl.gz;do
	echo ${f}
	python3 tbl_to_parquet.py ${f}
	rm ${f}
done

