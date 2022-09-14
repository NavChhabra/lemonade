#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No argument supplied. Please use --help for more info"
    exit 1
fi

spark-submit \
    --name "Lemonade" \
    --master local[*] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --deploy-mode client \
    --driver-memory 4G \
    --num-executors 2 \
    --executor-memory 4G \
    --executor-cores 4 \
    --files "./log4j.properties" \
    main.py "$@"