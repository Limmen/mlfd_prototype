# MLFD-Prototype and Testbed

 A testbed for running a simulated environment of processes and evaluate failure detectors.

 Two failure detectors are implemented and compared:

 - The classical eventual-perfect failure detector
 - Our own implementation of a machine-learning based failure detector

## How to run Simulations

1. **Set simulation parameters in:**`src/main/scala/kth/se/ii2202/mlfd_prototype/Main.scala`
2. **Build fat jar**
   - `$ sbt assembly`
3. **Run as spark-job**

**MLFD Example**
```
$SPARK_HOME/bin/spark-submit --class "kth.se.ii2202.mlfd_prototype.Main" target/scala-2.11/mlfd_prototype-assembly-0.1.0-SNAPSHOT.jar --test 1 --crash 0.001 --mloss 0.01 --sdev 100 --pmargin 3 --geof 1000.0 --geoc 20 --bwf 1000 --bwc 30 --pattern --workers 100 --sdevc 30 --hbtimeout 2 --testdur 30 --samplew 200 --defaultmean 3000 --defaultsdev 1000 --learnrate 0.0000000001 --regp 0.3 --iter 10 --batchsize 100 --distr 3
```
**EPFD Example**
```
$SPARK_HOME/bin/spark-submit --class "kth.se.ii2202.mlfd_prototype.Main" target/scala-2.11/mlfd_prototype-assembly-0.1.0-SNAPSHOT.jar --test 2 --crash 0.001 --mloss 0.01 --sdev 100 --delta 2000 --geof 1000.0 --geoc 20 --bwf 1000 --bwc 30 --pattern --workers 100 --sdevc 30 --hbtimeout 2 --testdur 30 --distr 3
```

## Simulation parameters

| Parameter-name | Description                                                                                                                   |
| -----          | -----------                                                                                                                   |
| --test         | [Int], Decides which FD to use, 1 for MLFD and 2 for EPFD [REQUIRED]                                                          |
| --crash        | [Double], Probability that a process crashes every time a heartbeat is received [REQUIRED]                                    |
| --mloss        | [Double], Probability of message loss for each heartbeat reply [REQUIRED]                                                     |
| --sdev         | [Double], Standard deviation factor to multiply each class of standard deviation in the simulation [REQUIRED]                 |
| --pmargin      | [Double], The number of standard-deviations used by MLFD as a margin for predictions [REQUIRED WHEN USING MLFD]               |
| --geof         | [Double], Factor to multiply each class of locations to get a geographic delay [REQUIRED]                                     |
| --geoc         | [Int], Number of geographic classes/locations to distribute nodes over  [REQUIRED]                                            |
| --bwf          | [Double], Factor to multiply each class of bandwidth to get bandwidth delay [REQUIRED]                                        |
| --bwc          | [Int], Number of bandwidth classes to distribute nodes over [REQUIRED]                                                        |
| --pattern      | [Boolean flag, no argument], indicates whether there should be a correlation between location, bandwidth and RTT [REQUIRED]   |
| --delta        | [Boolean] Milliseconds to increase timeout of EPFD [REQUIRED WHEN USING EPFD]                                                 |
| --workers      | [Int], Number of processes to detect failures of. [REQUIRED]                                                                  |
| --sdevc        | [Int], Number of standard-deviation classes to distribute nodes over [REQUIRED]                                               |
| --hbtimeout    | [Double], Number of seconds between each heartbeat timeout [REQUIRED]                                                         |
| --testdur      | [Double], Number of minutes to run the simulation before termination [REQUIRED]                                               |
| --samplew      | [Int], Size of sample-window used by MLFD to estimate mean and standard deviation. [REQUIREpD WHEN USING MLFD]                |
| --defaultmean  | [Double], Default mean to use for prediction before any samples are collected. [REQUIRED WHEN USING MLFD]                     |
| --defaultsdev  | [Double], Default standard deviation to use for prediction before any samples are collected [REQUIRED WHEN USING MLFD]        |
| --learnrate    | [Double], Learning-rate used by MLFD LinearRegressionWithSGD model. [REQUIRED WHEN USING MLFD]                                |
| --regp         | [Double], Regularization parameter used by MLFD LinearRegressionWithSGD model. [REQUIRED WHEN USING MLFD]                     |
| --iter         | [Int], Number of iterations used by MLFD LinearRegressionWithSGD model for each training-batch. [REQUIRED WHEN USING MLFD]    |
| --batchsize    | [Int], Training batch size used by MLFD LinearRegressionWithSGD model. [REQUIRED WHEN USING MLFD]                             |
| --distr        | [Int], Determines RTT delay distribution, 1=exponential,2=weibull,3=gaussian  [REQUIRED]                                      |

Data and statistics from simulation is written to csv files in `data/stats/`.

The files are overwritten for each simulation so remember to copy data to `data/backup/` if you want to save it.

## Data Analysis and Offline Machine Learning Models

See `jupyter/`

e.g [jupyter/Plots.ipynb](jupyter/Plots.ipynb)

## Authors

Kim Hammar, kimham@kth.se

Konstantin Sozinov, sozinov@kth.se
