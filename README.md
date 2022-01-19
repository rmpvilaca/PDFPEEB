# A simple benchmark for Python DataFrame frameworks

The benchmark uses the  data from the [NYC Taxi and Limousine Commission (TLC)](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset with billions of rows available.

Although all frameworks can read a CSV file, a more optimized approach is to use a binary version, namely [Parquet](https://parquet.apache.org/). Therefore, in the current version of the benchmark all implementations are using Parquet to load data except (Datatable, Sdc and Cylon) for each the method didn't exist/work.

Current frameworks:
* [Pandas](https://pandas.pydata.org/) The python data analysis library.
* [Intel® Scalable Dataframe Compiler](https://github.com/IntelPython/sdc) Extension of [Numba](https://numba.pydata.org/) that enables compilation of Pandas* operations. It automatically vectorizes and parallelizes the code.
* [Bodo](https://bodo.ai/) New approach to HPC-style parallel computing. Able to run distributed using MPI. Needs license.
* [Vaex](https://github.com/vaexio/vaex) High performance Python library for lazy Out-of-Core DataFrames.
* [Intel® Distribution of Modin](https://www.intel.com/content/www/us/en/developer/tools/oneapi/distribution-of-modin.html) A performant, parallel, and distributed dataframe system.
* [Dask](https://dask.org/) A Dask DataFrame is a large parallel DataFrame composed of many smaller Pandas DataFrames, split along the index. These Pandas DataFrames may live on disk for larger-than-memory computing on a single machine, or on many different machines in a cluster. 
* [Koalas](https://github.com/databricks/koalas) Implements the pandas DataFrame API on top of Apache Spark and thus able to run distributed.
* [Rapids](https://rapids.ai/) Open GPU Data Science. Able to run distributed with multiple GPUs when combined with Dask.
* [Polars](https://github.com/pola-rs/polars) Polars is a fast DataFrames library implemented in Rust using Apache Arrow Columnar Format as memory model.
* [Cylon](https://cylondata.org/) Is a data engineering toolkit designed to work with AI/ML systems and integrate with data processing systems. Able to run distributed using MPI.

## Requirements
The benchmark assumes the following packages are install:

* Python
* [PowerJoular](https://www.noureddine.org/research/joular/powerjoular). PowerJoular requires root/sudo access on the latest Linux kernels (5.10 and newer): sudo powerjoular. So if needed add sudo in run.sh and setup passwordless sudo.
* JDK for Koalas

## Operations

The benchmark currently support the  following common operations of dataframes:
* **join**: join with a very small dataframe with description of payment type.
* **mean**: mean of one column.
* **sum**: sum of several columns.
* **groupby**: mean of a given column grouped by other column.
* **multiple_groupby**:  mean of a given column grouped by other two columns.
* **unique_rows**:  Counts of unique values of a given column.
* **filter**: The filter operation filter the records to the ones that received a tip between $1 – 5 dollars, filtering down to 36% of the original data.


## Instructions for setup

All frameworks are loaded in a independent Conda environment apart from X that does not support Conda environment and thus we use regular Python virtual environment.

Thus the setup script creates an environment for each framework and also downloads the NYC TLC data to local parquet folders, for different dataset sizes.

```bash
setup.sh
```

## Instructions for running

Thus the run script uses the environments created for each framework by the setup script and the local data files download by the setup script, either to CSV or parquet files.

For each dataset size and framework, the script monitors the energy consumption using PowerJoular and store all results in a CSV file, one per dataset size.

```bash
run.sh
```

## Plotting results

The plot script generates the graphics in PNG and PDF format, taking as input the CSV files with results, one per dataset size.

```bash
plot.sh
```

## Feedback

Updated source and an issue tracker are available at [GitHub](https://github.com/rmpvilaca/PDFBench).

Your feedback is welcome.

## Contact

Ricardo Vilaca (<rmvilaca@di.uminho.pt>)