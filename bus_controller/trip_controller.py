import re
from imp import reload

import matplotlib.pyplot as plt
import pyspark.sql.dataframe
import seaborn as sns
from pyspark.sql.types import *
from pyspark.sql.functions import udf
# from pyspark.sql.functions import *
from pyspark.sql import functions as pyspark_func
from common.file_utils import FileUtils
from common.time_utils import TimeUtils
from common.geo_utils import GeoUtils
from Logger import logger


TRIPS_FNAME_PREFIX = 'metro-trips-'

TRIP_SCHEMA = StructType([
    StructField('trip_id', IntegerType(), False),
    StructField('duration', IntegerType(), True),
    StructField('start_time', StringType(), False),
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
    def __init__(self, spark, data_folder_name):
        logger.info("Initiating trip controller ...")

        self.__spark = spark
        self.__data_folder_name = data_folder_name
        self.init_udf()

        # Get data files name
        files_name = FileUtils.get_file_list_under_dir(self.__data_folder_name)
        # Filter files name start with "metro-trips"
        self.__trips_files_name = list(filter(lambda x: x.startswith(TRIPS_FNAME_PREFIX), files_name))
        self.__trips_dfs = dict()
        self.trips_total_df = None

    def init_udf(self):
        self.__udf_get_date = udf(lambda x: x.split(' ')[0] if ' ' in x else x, StringType())  # udf(TripUdf.get_date, StringType())
        self.__udf_format_time_to_datetime = udf(TripUdf.format_time_to_datetime, TimestampType())
        self.__udf_get_season = udf(TripUdf.get_season, IntegerType())
        self.__udf_get_holiday = udf(TripUdf.get_holiday, IntegerType())
        self.__udf_get_workingday = udf(TripUdf.get_workingday, IntegerType())
        self.__udf_cal_dist_by_lat_lon = udf(TripUdf.cal_dist_by_lat_lon, FloatType())
        self.__udf_cal_dist_by_lat_lon_cal = udf(TripUdf.cal_dist_by_lat_lon_cal, FloatType())

    def build_rdd(self):
        logger.info("Building trip RDD ...")
        # Build total RDD schema
        self.trips_total_df = self.__spark.createDataFrame(self.__spark.sparkContext.emptyRDD(), TRIP_SCHEMA
                                                           .add(StructField('distance', FloatType(), True))
                                                           .add(StructField('distance_cal', FloatType(), True))
                                                           .add(StructField('used_date', StringType(), True))
                                                           .add(StructField('season', IntegerType(), True))
                                                           .add(StructField('holiday', IntegerType(), True))
                                                           .add(StructField('workingday', IntegerType(), True))
                                                           .add(StructField('start_datetime', TimestampType(), True))
                                                           .add(StructField('end_datetime', TimestampType(), True)))\
            .withColumnRenamed('trip_route_category', 'trip_route_type')

        # Spark read csv
        for file_name in self.__trips_files_name:
            trip_date = re.match(r'.*(\d{4}-q\d{1}).*', file_name).group(1)
            self.__trips_dfs[trip_date] = self.__spark.read.options(header='True', inferSchema='True', delimiter=',').schema(
                TRIP_SCHEMA).csv("{}/{}.csv".format(self.__data_folder_name, file_name)).cache()
            '''
            cache():
                call persist(), persist() call persist(StorageLevel.MEMORY_ONLY)
                Persists the DataFrame with the default storage level (MEMORY_AND_DISK).
            '''
        logger.info('Finishing read trips data, total of files: {}.'.format(len(self.__trips_dfs)))

    def clean_data(self):
        logger.info("Cleaning trip data ...")
        if len(self.__trips_dfs) == 0:
            return

        for k, df in self.__trips_dfs.items():
            logger.info("Processing trip data: {}, lines={} ...".format(k, df.count()))
            self.__trips_dfs[k] = df.na.drop(subset=["start_lat", "start_lon", "end_lat", "end_lon"]) \
                .withColumn("distance", self.__udf_cal_dist_by_lat_lon("start_lat", "start_lon", "end_lat", "end_lon")) \
                .withColumn("distance_cal", self.__udf_cal_dist_by_lat_lon_cal("start_lat", "start_lon", "end_lat", "end_lon")) \
                .withColumn("used_date", self.__udf_get_date("start_time")) \
                .withColumn("season", self.__udf_get_season("used_date")) \
                .withColumn("holiday", self.__udf_get_holiday("used_date")) \
                .withColumn("workingday", self.__udf_get_workingday("used_date")) \
                .withColumn("start_datetime", self.__udf_format_time_to_datetime("start_time")) \
                .withColumn("end_datetime", self.__udf_format_time_to_datetime("end_time")) \
                .withColumn("trip_route_category", pyspark_func.when(df.trip_route_category == 'One Way', 1).when(df.trip_route_category == 'Round Trip', 2)) \
                .withColumn("passholder_type", pyspark_func.when(df.passholder_type == 'Walk-up', 1).when(df.passholder_type == 'One Day Pass', 2)
                            .when(df.passholder_type == 'Monthly Pass', 3).when(df.passholder_type == 'Annual Pass', 4)) \
                .withColumn("bike_type", pyspark_func.when(df.bike_type == 'standard', 1).when(df.bike_type == 'electric', 2)
                            .when(df.bike_type == 'smart', 3)) \
                .withColumnRenamed('trip_route_category', 'trip_route_type')
            # .filter("distance != 0.0") \  ### cannot filter because round-trip have the same start and end stations
            # .cast(DateType()) # .drop('col_name') # .filter(df.distance != 0.0)
            logger.info("Processing trip data success: {}, lines={} ...".format(k, self.__trips_dfs[k].count()))

            trip_df_block = self.__trips_dfs[k]
            self.trips_total_df = self.trips_total_df.unionAll(trip_df_block)
            logger.info("Processing trip data {}: total size={} ...".format(k, self.trips_total_df.count()))

        logger.info('Trip total RDD schema:')
        self.print_schema(self.trips_total_df)
        logger.info('Trip total RDD data:')
        self.trips_total_df.show(truncate=True)

    def exp_total_to_csv(self):
        logger.info('Export total trip data to csv')
        try:
            export_csv_path = 'results/trips'
            if FileUtils.path_exists(export_csv_path):
                FileUtils.remove_folder(export_csv_path)
            self.trips_total_df.repartition(1).write.csv(export_csv_path, encoding="utf-8", header=True)
        except pyspark.sql.utils.AnalysisException as e:
            logger.error('Trip export csv error: {}'.format(e))

    def print_schema(self, df):
        df.printSchema()
        # print(self.__trips_dfs['2021-q1'].schema)
        # print('Tables: {}'.format(spark.catalog.listTables()))
        # print(TRIP_SCHEMA.simpleString())

    def stat_basic(self, df):
        print(df.describe().show())

    def countplot_by_category(self):
        fig, axes = plt.subplots(nrows=3, ncols=0)
        fig.set_size_inches(16, 8)
        for k, df in self.__trips_dfs.items():
            logger.info('countplot in categorical variable: {}'.format(k))
            sns.countplot(df['season'], ax=axes[0][0])
            sns.countplot(df['holiday'], ax=axes[1][0])
            sns.countplot(df['workingday'], ax=axes[2][0])

    def ctor(self):
        self.__trips_dfs = None
        self.trips_total_df = None


class TripUdf:
    @staticmethod
    def get_date(time_str: str) -> str:
        date_str, time_str = time_str.split(' ')
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

    @staticmethod
    def cal_dist_by_lat_lon(lat1: float, lon1: float, lat2: float, lon2: float, ) -> float:
        if not lat1 or not lon1 or not lat2 or not lon2:
            print('Func cal_dist_by_lat_lon: params None, lat1={}, lon1={}, lat2={}, lon2={}'.format(lat1, lon1, lat2, lon2))
            return 0.0
        return GeoUtils.get_distance_by_lng_lat(lat1, lon1, lat2, lon2, unit='m')

    @staticmethod
    def cal_dist_by_lat_lon_cal(lat1: float, lon1: float, lat2: float, lon2: float, ) -> float:
        if not lat1 or not lon1 or not lat2 or not lon2:
            print('Func cal_dist_by_lat_lon_cal: params None, lat1={}, lon1={}, lat2={}, lon2={}'.format(lat1, lon1, lat2, lon2))
            return 0.0
        return GeoUtils.get_distance_by_lng_lat_cal(lat1, lon1, lat2, lon2)
