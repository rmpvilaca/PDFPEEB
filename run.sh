#!/bin/bash

#Start DStat
dstat --output dstat.csv &
DSTAT_PID=$! 

#Setup conda
source ~/anaconda3/etc/profile.d/conda.sh
#source ~/miniconda3/etc/profile.d/conda.sh

DATASETS="yellow_tripdata_2019_01-2019_6 yellow_tripdata_2019_01-2019_12 yellow_tripdata_2019_01-2020_12"
CONDA_FRAMEWORKS="Vaex Bodo Koalas Modin Datatable Cylon DuckDB Rapids" # Rapids Sdc
VENV_FRAMEWORKS="Polars Pandas"

for DATASET in $DATASETS; do
    echo "Running $DATASET"
    rm energy_*.csv
    rm results_$DATASET.csv

    for CONDA_FRAMEWORK in $CONDA_FRAMEWORKS; do
        echo "Running $CONDA_FRAMEWORK"
        conda activate $CONDA_FRAMEWORK
        python src/benchmark.py --dataset $DATASET --framework $CONDA_FRAMEWORK  > $CONDA_FRAMEWORK.out 2>&1 &
        PID=$!
        powerjoular -f energy_$CONDA_FRAMEWORK.csv &
        PJ_PID=$!
        wait $PID
        kill $PJ_PID
        ENERGY="$(LC_NUMERIC="C" awk -F , '(NR!=1) {sum+=$3} END {print sum/NR}' energy_$CONDA_FRAMEWORK.csv)"
        sed '$s/$/,'"$ENERGY"'/' results_$DATASET.csv > tmp
        mv tmp results_$DATASET.csv
        conda deactivate
    done

    for VENV_FRAMEWORK in $VENV_FRAMEWORKS; do
        echo "Running $VENV_FRAMEWORK"
        source $VENV_FRAMEWORK/bin/activate
        python src/benchmark.py --dataset $DATASET --framework $VENV_FRAMEWORK  > $VENV_FRAMEWORK.out 2>&1 &
        PID=$! 
        powerjoular -f energy_$VENV_FRAMEWORK.csv &
        PJ_PID=$! 
        wait $PID
        kill $PJ_PID 
        ENERGY="$(LC_NUMERIC="C" awk -F , '(NR!=1) {sum+=$3} END {print sum/NR}' energy_$VENV_FRAMEWORK.csv)"
        sed '$s/$/,'"$ENERGY"'/' results_$DATASET.csv > tmp
        mv tmp results_$DATASET.csv
        deactivate
    done

    WORKERS_BODO="4 8"
    CONDA_FRAMEWORK="Bodo"
    for WORKER in $WORKERS_BODO; do
        echo "Running $CONDA_FRAMEWORK with $WORKER workers"
        conda activate $CONDA_FRAMEWORK
        mpiexec -n $WORKER python src/benchmark.py --dataset $DATASET --framework $CONDA_FRAMEWORK --workers $WORKER > $CONDA_FRAMEWORK-$WORKER.out 2>&1  &
        PID=$!
        powerjoular -f energy_$CONDA_FRAMEWORK_$WORKER.csv &
        PJ_PID=$!
        wait $PID
        kill $PJ_PID
        ENERGY="$(LC_NUMERIC="C" awk -F , '(NR!=1) {sum+=$3} END {print sum/NR}' energy_$CONDA_FRAMEWORK_$WORKER.csv)"
        sed '$s/$/,'"$ENERGY"'/' results_$DATASET.csv > tmp
        mv tmp results_$DATASET.csv
        conda deactivate
    done
    WORKERS_DASK="4 8 16"
    CONDA_FRAMEWORK="Dask"
    for WORKER in $WORKERS_DASK; do
        echo "Running $CONDA_FRAMEWORK with $WORKER workers"
        conda activate $CONDA_FRAMEWORK
        python src/benchmark.py --dataset $DATASET --framework $CONDA_FRAMEWORK --workers $WORKER > $CONDA_FRAMEWORK-$WORKER.out 2>&1  &
        PID=$!
        powerjoular -f energy_$CONDA_FRAMEWORK_$WORKER.csv &
        PJ_PID=$!
        wait $PID
        kill $PJ_PID
        ENERGY="$(LC_NUMERIC="C" awk -F , '(NR!=1) {sum+=$3} END {print sum/NR}' energy_$CONDA_FRAMEWORK_$WORKER.csv)"
        sed '$s/$/,'"$ENERGY"'/' results_$DATASET.csv > tmp
        mv tmp results_$DATASET.csv
        conda deactivate
    done
done

# Kill DSTAT
kill $DSTAT_PID
