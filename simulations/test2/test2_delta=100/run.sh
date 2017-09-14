#!/bin/bash

$SPARK_HOME/bin/spark-submit --class "kth.se.ii2202.mlfd_prototype.Main" mlfd_prototype-assembly-0.1.0-SNAPSHOT.jar --test 2 --crash 0.001 --mloss 0.01 --sdev 100 --delta 100 --geof 50.0 --geoc 20 --bwf 1000 --bwc 30 --pattern --workers 100 --sdevc 30 --hbtimeout 2 --testdur 10
