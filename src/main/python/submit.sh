#!/usr/bin/env bash
spark-submit pyspark_cube/build.py \
            --input ~/scripts/spark/data/data.csv \
            --output ~/scripts/spark/out \
            --yyyymm 201506 \
            --combo ~/scripts/spark/data/combo.csv
