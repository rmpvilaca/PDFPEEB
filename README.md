# A simple benchmark for Python DataFrame frameworks

The benchmark uses the  data from the [NYC Taxi and Limousine Commission (TLC)](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset with billions of rows available.

Although all frameworks can read a CSV file, a more optimized approach is to use a binary version, namely [Parquet](https://parquet.apache.org/). Therefore, in the current version of the benchmark all implementations are using Parquet to load data except (Datatable and Sdc) for each the method didn't exist/work.

Current frameworks:
* [Pandas](https://pandas.pydata.org/)
* [Intel® Scalable Dataframe Compiler](https://github.com/IntelPython/sdc)
* [Bodo](https://bodo.ai/). Able to run distributed using MPI.
* [Vaex](https://github.com/vaexio/vaex)
* [Intel Distribution of Modin](https://www.intel.com/content/www/us/en/developer/tools/oneapi/distribution-of-modin.html)
* [Dask](https://dask.org/): Able to run distribute.
* [Koalas](https://github.com/databricks/koalas).  Able to run distribute using Spark.
* [Rapids](https://rapids.ai/). Able to run distributed with multiple GPUs when combined with Dask.
* [Polars](https://github.com/pola-rs/polars)
* [Cylon](https://cylondata.org/). Able to run distributed using MPI.

## Requirements
The benchmark assumes the following packages are install:

* Python
* [PowerJoular](https://www.noureddine.org/research/joular/powerjoular). PowerJoular requires root/sudo access on the latest Linux kernels (5.10 and newer): sudo powerjoular. So if needed add sudo in run.sh and setup passwordless sudo.
* JDK for Koalas

## Operations

The benchmark currently support the  following common operations of dataframes:
* **join**: join with a very small datfram with description of payment type.
* **mean**: mean of one column.
* **sum**: sum of several columns.
* **groupby**: mean of a given column grouped by other column.
* **multiple_groupby**:  mean of a given column grouped by other two columns.
* **unique_rows**:  Counts of unique values of a given column.
* **filter**: The filter operation finds the records that received a tip between $1 – 5 dollars, and it filters down to 36% of the original data.


## Instructions for setup

All frameworks are loaded in a independent Conda environment apart from X that does not support Conda environment and thus we use regular Python virtual environment.

Thus the setup script creates an environment for each framework and also downloads the NYC TLC data to local parquet folders, for different dataset sizes.

{% highlight bash %}
setup.sh
{% endhighlight %}

## Instructions for running

Thus the run script uses the environments created for each framework by the setup script and the local data files download by the setup script, either to CSV or parquet files.

For each dataset size and framework, the script monitors the energy consumption using PowerJoular and store all results in a CSV file, one per dataset size.

{% highlight bash %}
run.sh
{% endhighlight %}

## Plotting results

The plot script generates the graphics in PNG and PDF format, taking as input the CSV files with results, one per dataset size.

{% highlight bash %}
plot.sh
{% endhighlight %}

## Feedback

Updated source and an issue tracker are available at:

        https://github.com/rmpvilaca/PDFBench

Your feedback is welcome.

## Contact

Ricardo Vilaca (<rmvilaca@di.uminho.pt>)