#!/bin/bash
spark-submit --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 1g  --properties-file /app/configs/nasa_log_conf.properties /app/src/nasa_log_analysis.py