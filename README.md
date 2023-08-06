
# Shared Bike Analysis System

The Shared Bike Analysis System is designed to process and analyze shared bike usage data. The system requires Python 3 and Java 8 (due to Spark's dependency). It includes a variety of tools and services such as Apache Spark, Hadoop, Scala, and more.

## Table of Contents

- [Environment Setup](#environment-setup)
- [Data Source](#data-source)
- [Installation](#installation)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Additional Information](#additional-information)

## Environment Setup

The project's main script is `main.py`, and Jupyter Notebook visualization is provided via `test.ipynb`. 

**Note:** Spark only supports Java 8 (not JDK 11).

## Data Source

The data directory is located at `./data`. Please ensure your data files are placed here. 

Our primary data source is the Metro Bike Share [data site](https://bikeshare.metro.net/about/data/). We are only using data from the past three years, as older data may have different formats. 

You can also download the data from our GitHub repository, [here](https://github.com/AstroMen/SharedBikeAnalysisSystem/tree/main/data).

## Installation 

### Spark, Hadoop, and Scala Setup

Configure the environment variables as follows:

```shell
# spark
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.0
export PATH=$PATH:$SPARK_HOME/bin
export PYTHONPATH=$SPARK_HOME/libexec/python
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.2-src.zip:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/build:$PYTHONPATH

# hadoop
export HADOOP_HOME=/usr/local/Cellar/hadoop/3.3.1
export PATH=$PATH:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# scala
export SCALA_HOME=/usr/local/Cellar/scala/2.13.7
export PATH=$PATH:$SCALA_HOME/bin
```

### Python Package Installation

Install the `virtualenv` package using pip:

```shell
pip install virtualenv
```

## Usage

### Python Virtual Environment

A `venv` directory is provided, which includes all necessary Python packages. 

To activate the virtual environment, use: `source venv/bin/activate` 

To exit the virtual environment, use: `deactivate`

### Running the Code

To generate the tables, use: 

```shell
spark-submit main.py
```

### Visualization

To start the visualization tool, use: 

```shell
flask run --host=0.0.0.0 --port=5000
```

Then, access it in your browser at: `http://0.0.0.0:8888/notebooks/test.ipynb`

## Troubleshooting

If you encounter issues, please check the following potential solutions:

1. If you encounter the error "ERROR XSDB6: Another instance of Derby may have already booted the database":

    ```shell
    ps -ef | grep spark-shell
    kill -9 <processID>
    ```

2. If you get the "Java version unsupported" error from `org.apache.spark.storage.StorageUtils`, please ensure you're using Java 8.

3. If you need to use a specific Java version, you can modify the `java8_location` variable in `spark_util.py` to set the `JAVA_HOME` for this program.

4. If you're having trouble with Hive, try deleting `db.lck` and `dbex.lck` in the `metastore_db` directory.

5. For other issues, please submit them [here](https://github.com/AstroMen/SharedBikeAnalysisSystem/issues).

## Additional Information

### Other Data Sources

- [Lyft Bay Wheels System Data](https://www.lyft.com/bikes/bay-wheels/system-data)

### Future Work

We plan to add weather features to the analysis. Potential weather data sources include:

- [FiveThirtyEight US Weather History](https://data.world/fivethirtyeight/us-weather-history)
- [CIMIS Weather Station Data](https://data.ca.gov/dataset/cimis-weather-station-data1)
- [Weather Underground KLAX Data](https://www.wunderground.com/weather/KLAX)
- [NOAA GHCN PDS](https://docs.opendata.aws/noaa-ghcn-pds/readme.html)
- [Daily Weather Station Data](ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/)

### Related Projects

- [Bay Wheels Visualization](https://www.visualization.bike/baywheels/overview/2020/)
