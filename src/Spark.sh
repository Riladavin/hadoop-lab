#!/bin/bash

apk add --update make automake gcc g++
apk add --update python-dev
apk add linux-headers
pip3 install numpy psutil scikit-learn
for ((i=1; i<=20; i++))
do
    /spark/bin/spark-submit /spark_app.py $1
done
exit