import re
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from common.file_utils import FileUtils
from common.time_utils import TimeUtils
from Logger import logger


DATA_FLODER_NAME = 'data_test'

TRIP_SCHEMA = StructType([
    StructField('trip_id', IntegerType(), False),
    StructField('duration', IntegerType(), True),
    StructField('start_time', StringType(), True),  # DateType
    StructField('end_time', StringType(), True),
    StructField('start_station', IntegerType(), True),
    StructField('start_lat', DoubleType(), True),
    StructField('start_lon', DoubleType(), True),
    StructField('end_station', IntegerType(), True),
    StructField('end_lat', DoubleType(), True),
    StructField('end_lon', DoubleType(), True),
    StructField('bike_id', IntegerType(), True),
    StructField('plan_duration', IntegerType(), True),
    StructField('trip_route_category', StringType(), True),
    StructField('passholder_type', StringType(), True),
    StructField('bike_type', StringType(), True),
])


class TripController:
    def __init__(self, spark):
        logger.info("Initiating trip controller ...")

        # Get data files name
        files_name = FileUtils.get_file_list_under_dir(DATA_FLODER_NAME)
        # Filter files name start with "metro-trips"
        trips_fname_prefix = 'metro-trips-'
        trips_files_name = list(filter(lambda x: x.startswith(trips_fname_prefix), files_name))

        # Spark read csv
        trips_dfs = dict()
        for file_name in trips_files_name:
            trip_date = re.match(r'.*(\d{4}-q\d{1}).*', file_name).group(1)
            trips_dfs[trip_date] = spark.read.options(header='True', inferSchema='True', delimiter=',').schema(
                TRIP_SCHEMA).csv("{}/{}.csv".format(DATA_FLODER_NAME, file_name))
        logger.info('Finishing read trips data, total of files: {}.'.format(len(trips_dfs)))

        # Clean data
        udf_get_date = udf(TripUdf.get_date, StringType())
        udf_format_time_to_datetime = udf(TripUdf.format_time_to_datetime, TimestampType())
        udf_get_season = udf(TripUdf.get_season, IntegerType())
        udf_get_holiday = udf(TripUdf.get_holiday, IntegerType())
        udf_get_workingday = udf(TripUdf.get_workingday, IntegerType())
        for k, df in trips_dfs.items():
            # trips_dfs[k] = df.withColumn("season", udf_get_season("start_time"))
            trips_dfs[k] = df.withColumn("used_date", udf_get_date("start_time")) \
                .withColumn("season", udf_get_season("used_date")) \
                .withColumn("holiday", udf_get_holiday("used_date")) \
                .withColumn("workingday", udf_get_workingday("used_date")) \
                .withColumn("start_datetime", udf_format_time_to_datetime("start_time")) \
                .withColumn("end_datetime", udf_format_time_to_datetime("end_time"))  # .cast(DateType()) # .drop('col_name')

        trips_dfs['2021-q1'].show()
        trips_dfs['2021-q1'].printSchema()
        print('Tables: {}'.format(spark.catalog.listTables()))
        # print(trips_dfs['2021-q1'].schema)
        # print(TRIP_SCHEMA.simpleString())

    def countplot_by_category(self):
        fig, axes = plt.subplots(nrows=3, ncols=0)
        fig.set_size_inches(16, 8)

        sns.countplot(train['season'], ax=axes[0][0])
        sns.countplot(train['holiday'], ax=axes[0][1])
        sns.countplot(train['workingday'], ax=axes[1][0])
        sns.countplot(train['weather'], ax=axes[1][1])


class TripUdf:
    @staticmethod
    def get_date(time_str: str) -> str:
        date_str, time_str = time_str.split(' ')
        # month_str, day_str, year_str = time_str.split(' ')[0].split('/')
        return date_str

    @staticmethod
    def format_time_to_datetime(time_str: str) -> TimestampType:
        return TimeUtils.string_toDatetime(time_str, format_str="%m/%d/%Y %H:%M", is_check=False)

    @staticmethod
    def get_season(date_str: str) -> int:
        # Winter: 0, Spring: 1, Summer: 2, Fall: 3
        # date_str, time_str = time_str.split(' ')
        month_str, day_str, year_str = date_str.split('/')
        month, day, year = int(month_str), int(day_str), int(year_str)
        if month in [1, 2, 4, 5, 7, 8, 10, 11]:
            season = month / 3 + 1
        elif month in [3, 6, 9]:
            season = month / 3 if day < 21 else month / 3 + 1
        elif month == 12:
            season = 4 if day < 21 else 1
        else:
            season = 0.0
        return int(season)

    @staticmethod
    def get_holiday(date_str: str) -> int:
        month_str, day_str, year_str = date_str.split('/')
        return 1 if TimeUtils.is_usa_holiday(int(year_str), int(month_str), int(day_str)) else 0

    @staticmethod
    def get_workingday(date_str: str) -> int:
        month_str, day_str, year_str = date_str.split('/')
        if TimeUtils.is_business_day('{}-{}-{}'.format(year_str, month_str, day_str)) and \
            not TimeUtils.is_usa_holiday(int(year_str), int(month_str), int(day_str)):
            return 1
        else:
            return 0

