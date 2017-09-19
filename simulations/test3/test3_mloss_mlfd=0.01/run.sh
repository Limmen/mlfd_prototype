#!/bin/bash

$SPARK_HOME/bin/spark-submit --class "kth.se.ii2202.mlfd_prototype.Main" mlfd_prototype-assembly-0.1.0-SNAPSHOT.jar --test 1 --crash 0.001 --mloss 0.01 --sdev 100 --pmargin 3 --geof 1000.0 --geoc 20 --bwf 1000 --bwc 30 --workers 100 --sdevc 30 --hbtimeout 2 --testdur 30 --samplew 200 --defaultmean 3000 --defaultsdev 1000 --learnrate 0.0000000001 --regp 0.3 --iter 10 --batchsize 100 --distr 3 --pattern
