# MLFD

## How to run Simulations

1. **Set simulation parameters in `src/main/scala/kth/se/ii2202/mlfd_prototype/Main.scala`**
2. **Build fat jar**
   - `$ sbt assembly`
3. **Run as spark-job**
```
$SPARK_HOME/bin/spark-submit --class ""kth.se.ii2202.mlfd_prototype.Main"" target/scala-2.11/mlfd_prototype-assembly-0.1.0-SNAPSHOT.jar
```

Data and statistics from simulation is written to csv files in `data/stats/`.

The files are overwritten for each simulation so remember to copy data to `data/backup/` if you want to save it.

## Data Analysis and Offline Machine Learning Models

See `jupyter/`

e.g [jupyter/Plots.ipynb](jupyter/Plots.ipynb)
