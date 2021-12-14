# SharedBikeAnalysisSystem

## Environment Description
This project just support python3.  
Project startup: main.py    
Jupyter visualization: test.ipynb   
Spark only support Java8(jdk11)

## data source
Data directory is ./data, you should put data files in this directory.
From: https://bikeshare.metro.net/about/data/   
We just use recent 3 years data, and old data maybe have different data format.  
You could also download data from our git website: https://github.com/AstroMen/SharedBikeAnalysisSystem/tree/main/data

## Install spark, hadoop, scala
### Config environment variable
```shell
# spark
SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.0
export PATH=$PATH:$SPARK_HOME/bin
export PYTHONPATH=$SPARK_HOME/libexec/python
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.2-src.zip:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/build:$PYTHONPATH

# hadoop
HADOOP_HOME=/usr/local/Cellar/hadoop/3.3.1
export PATH=$PATH:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# scala
SCALA_HOME=/usr/local/Cellar/scala/2.13.7
export PATH=$PATH:$SCALA_HOME/bin
```

## Install python package
pip install virtualenv

## Run in python virtual env
There is a directory of venv to provide completed python running packages.    
Convert to virtual env: source venv/bin/activate   
Quit virtual env: deactivate

## Run code (Execute task to generate tables)
spark-submit main.py

## Startup visualization
Run: flask run --host=0.0.0.0 --port=5000    
Browser access to: http://0.0.0.0:8888/notebooks/test.ipynb

## TroubleShooting    
1. Caused by: ERROR XSDB6: Another instance of Derby may have already booted the database.  
    ``` shell
    ps -ef | grep spark-shell
    kill -9 processID
    ```
2. org.apache.spark.storage.StorageUtils Error: Java version unsupported.
3. If you want to use a specific java version, you could refer variable 'java8_location' in spark_util.py to set JAVA_HOME for this program.
4. If something wrong with hive, you could try to delete db.lck and dbex.lck in the directory of 'metastore_db'.
5. Other issues: could submit to https://github.com/AstroMen/SharedBikeAnalysisSystem/issues

# Others
Other public data source: https://www.lyft.com/bikes/bay-wheels/system-data

Future work: add weather feature to analysis.   
Weather data source:    
https://data.world/fivethirtyeight/us-weather-history   
https://data.ca.gov/dataset/cimis-weather-station-data1 
https://www.wunderground.com/weather/KLAX   
https://docs.opendata.aws/noaa-ghcn-pds/readme.html     
daily weather station data: ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/    

Related project:    
https://www.visualization.bike/baywheels/overview/2020/
