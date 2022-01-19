#!/bin/bash

source ~/anaconda3/etc/profile.d/conda.sh

# Create Conda environments
conda env create --file envs/env_sdc.yml
conda env create --file envs/env_vaex.yml
conda env create --file envs/env_bodo.yml
conda env create --file envs/env_modin.yml
conda env create --file envs/env_dask.yml
conda env create --file envs/env_rapids.yml
conda env create --file envs/env_koalas.yml
conda env create --file envs/env_datatable.yml
conda env create --file envs/env_cylon.yml
conda env create --file envs/env_duckDB.yml

# Create Polars venv
python3 -m venv Polars
source Polars/bin/activate
python -m pip install --upgrade py-polars
python -m pip install --upgrade pandas
deactivate

# Create Pandas venv
python3 -m venv Pandas
source Pandas/bin/activate
python -m pip install --upgrade pandas
python -m pip install --upgrade pyarrow
deactivate

# Create Plot venv
python3 -m venv plot
source plot/bin/activate
python -m pip install --upgrade numpy
python -m pip install --upgrade matplotlib
deactivate

#Download NYC TLC data to local parquet folders
source Pandas/bin/activate
python src/download-NYC-TLC-data.py
deactivate
