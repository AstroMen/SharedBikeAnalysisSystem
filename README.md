# SharedBikeAnalysisSystem

This project just support python3.  
Project startup: main.py    
Jupyter visualization: test.ipynb

Data source from: https://bikeshare.metro.net/about/data/   
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

TroubleShooting:    
1. Caused by: ERROR XSDB6: Another instance of Derby may have already booted the database  
    ``` shell
    ps -ef | grep spark-shell
    kill -9 processID
    ```
