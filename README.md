# MLFD-Prototype and Testbed

 A testbed for running a simulated environment of processes and evaluate failure detectors.

 Two failure detectors are implemented and compared:

 - The classical eventual-perfect failure detector
 - Our own implementation of a machine-learning based failure detector

## How to run Simulations

1. **Set simulation parameters in `src/main/scala/kth/se/ii2202/mlfd_prototype/Main.scala`**
2. **Build fat jar**
   - `$ sbt assembly`
3. **Run as spark-job**
```
$SPARK_HOME/bin/spark-submit --class "kth.se.ii2202.mlfd_prototype.Main" target/scala-2.11/mlfd_prototype-assembly-0.1.0-SNAPSHOT.jar --test <testnum> --crash <crashProb> --mloss <messageLossProb>
```

## Available simulations

| Simulation                      | SimulationNum |
| ------------------------------- | ------------- |
| ClearCorrelationSimulationMLFD  | 1             |
| ClearCorrelationSimulationEPFD  | 2             |
| RandomCorrelationSimulationMLFD | 3             |
| RandomCorrelationSimulationEPFD | 4             |

Data and statistics from simulation is written to csv files in `data/stats/`.

The files are overwritten for each simulation so remember to copy data to `data/backup/` if you want to save it.

## Data Analysis and Offline Machine Learning Models

See `jupyter/`

e.g [jupyter/Plots.ipynb](jupyter/Plots.ipynb)

## Authors

Kim Hammar, kimham@kth.se

Konstantin Sozinov, sozinov@kth.se
