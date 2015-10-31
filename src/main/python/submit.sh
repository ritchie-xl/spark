#!/usr/bin/env bash
spark-submit adp_cube/benchmark_cube_build.py \
            --input ~/scripts/spark/data/data.csv \
            --output ~/scripts/spark/src/main/python/out \
            --quarter 2015Q2 \
            --combo ~/scripts/spark/src/main/python/data/combo.csv \
			--target gross_wage \
			--master local

