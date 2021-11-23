import re

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
from cluster_util.hive_util import HiveUtil
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
    def __init__(self, spark, hive, data_folder_name):
        logger.info("Initiating trip controller ...")

        self.__spark = spark
        self.__hive = hive
        self.__data_folder_name = data_folder_name
        self.init_udf()

        # Get data files name
        files_name = FileUtils.get_file_list_under_dir(self.__data_folder_name)
        # Filter files name start with "metro-trips"
        self.__trips_files_name = list(filter(lambda x: x.startswith(TRIPS_FNAME_PREFIX), files_name))
        self.__trips_dfs = dict()
        self.trips_total_df = None
        self.ptd = list()
        # Build total RDD schema
        self.trips_total_df = self.__spark.createDataFrame(self.__spark.sparkContext.emptyRDD(), TRIP_SCHEMA
                                                           .add(StructField('distance', FloatType(), True))
                                                           .add(StructField('distance_cal', FloatType(), True))
                                                           .add(StructField('used_date', StringType(), True))
                                                           .add(StructField('season', IntegerType(), True))
                                                           .add(StructField('holiday', IntegerType(), True))
                                                           .add(StructField('workingday', IntegerType(), True))
                                                           .add(StructField('start_datetime', TimestampType(), True))
                                                           .add(StructField('end_datetime', TimestampType(), True))
                                                           .add(StructField('start_hour', StringType(), True)))\
            .withColumnRenamed('trip_route_category', 'trip_route_type')

    def init_udf(self):
        self.__udf_get_date = udf(lambda x: x.split(' ')[0] if ' ' in x else x, StringType())  # udf(TripUdf.get_date, StringType())
        self.__udf_get_used_time = udf(TripUdf.get_used_time, IntegerType())
        self.__udf_get_year = udf(lambda x: x.split('/')[2] if '/' in x else 'UnknownYear', StringType())
        self.__udf_format_time_to_datetime = udf(TripUdf.format_time_to_datetime, TimestampType())
        self.__udf_format_time_to_hour_str = udf(TripUdf.format_time_to_hour_str, StringType())
        self.__udf_get_season = udf(TripUdf.get_season, IntegerType())
        self.__udf_get_holiday = udf(TripUdf.get_holiday, IntegerType())
        self.__udf_get_workingday = udf(TripUdf.get_workingday, IntegerType())
        self.__udf_cal_dist_by_lat_lon = udf(TripUdf.cal_dist_by_lat_lon, FloatType())
        self.__udf_cal_dist_by_lat_lon_cal = udf(TripUdf.cal_dist_by_lat_lon_cal, FloatType())

    def build_dw(self):
        logger.info("Building trip DW ...")
        # Spark read csv
        for file_name in self.__trips_files_name:
            logger.info('Reading file {} ...'.format(file_name))
            trip_date = re.match(r'.*(\d{4}-q\d{1}).*', file_name).group(1)
            self.__trips_dfs[trip_date] = self.__spark.read.options(header='True', inferSchema='True', delimiter=',').schema(
                TRIP_SCHEMA).csv("{}/{}.csv".format(self.__data_folder_name, file_name)).cache()
            '''
            cache():
                call persist(), persist() call persist(StorageLevel.MEMORY_ONLY)
                Persists the DataFrame with the default storage level (MEMORY_AND_DISK).
            '''

            self.clean_ods_data(trip_date, self.__trips_dfs[trip_date])
            self.__trips_dfs[trip_date].show()
            self.store_dw_to_hive(self.__trips_dfs[trip_date], trip_date.split('-')[0], 'tmp_{}'.format(trip_date.replace('-', '_')))

        # trip_df_block = self.__trips_dfs[k]
        # self.trips_total_df = self.trips_total_df.unionAll(trip_df_block)
        # logger.info("Processing trip data {}: total size={} ...".format(k, self.trips_total_df.count()))
        #
        # logger.info('Trip total RDD schema:')
        # self.print_schema(self.trips_total_df)
        # logger.info('Trip total RDD data:')
        # self.trips_total_df.show(truncate=True)

        logger.info('Finishing build odf for trips data, total of files: {}.'.format(len(self.__trips_dfs)))

    def clean_ods_data(self, k, df):
        logger.info("Cleaning trip data: {} ...".format(k))
        if df.count() == 0:
            return

        # for k, df in self.__trips_dfs.items():
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
            .withColumn("start_hour", self.__udf_format_time_to_hour_str("start_time")) \
            .withColumn("trip_route_category", pyspark_func.when(df.trip_route_category == 'One Way', 1).when(df.trip_route_category == 'Round Trip', 2)) \
            .withColumn("passholder_type", pyspark_func.when(df.passholder_type == 'Walk-up', 1).when(df.passholder_type == 'One Day Pass', 2)
                        .when(df.passholder_type == 'Monthly Pass', 3).when(df.passholder_type == 'Annual Pass', 4)) \
            .withColumn("bike_type", pyspark_func.when(df.bike_type == 'standard', 1).when(df.bike_type == 'electric', 2)
                        .when(df.bike_type == 'smart', 3)) \
            .withColumnRenamed('trip_route_category', 'trip_route_type').drop('start_time').drop('end_time')
        # .withColumn("used_time", self.__udf_get_used_time("start_datetime", "end_datetime")) \
        # .filter("distance != 0.0") \  ### cannot filter because round-trip have the same start and end stations
        # .cast(DateType()) # .drop('col_name') # .filter(df.distance != 0.0)
        logger.info("Processing trip data success: {}, lines={} ...".format(k, self.__trips_dfs[k].count()))

    def store_dw_to_hive(self, df, ptd, tmp_tb_name):
        logger.info('Importing {} to hive ...'.format(tmp_tb_name))
        is_first_insert = True
        df.createOrReplaceTempView(tmp_tb_name)

        if ptd in self.ptd:
            is_first_insert = False
        else:
            self.ptd.append(ptd)

        # crt_tb_sql = """
        #     CREATE TABLE IF NOT EXISTS SharedBike.trip_details (trip_id INT, value STRING) USING hive
        # """

        crt_tb_sql = """
                    create table IF NOT EXISTS SharedBike.trip_details like {} USING hive
                """.format(tmp_tb_name)  # PARTITIONED BY (ptd String)

        crt_tb_sql = """
        CREATE TABLE IF NOT EXISTS SharedBike.trip_details (trip_id int, duration int, start_station int, start_lat double, 
        start_lon double, end_station int, end_lat double, end_lon double, bike_id int, plan_duration int, 
        trip_route_type int, passholder_type int, bike_type int, distance float, distance_cal float, used_date string, 
        season int, holiday int, workingday int, start_datetime timestamp, end_datetime timestamp, start_hour string)
        PARTITIONED BY (ptd String)
        """
        self.__spark.sql(crt_tb_sql)

        ist_sql = """
                    insert {mode} table SharedBike.trip_details partition(ptd='{partition}') select * from {src_tb_name}
            """.format(mode='overwrite' if is_first_insert else 'INTO', partition=ptd, src_tb_name=tmp_tb_name)
        self.__spark.sql(ist_sql)

        # self.__hive.exec_sql(''' create table SharedBike.trip_details like {} '''.format(tmp_tb_name))
        # self.__hive.exec_sql(''' insert overwrite table SharedBike.trip_details select * from {} '''.format(tmp_tb_name))
        select_sql = '''select count(1) from SharedBike.trip_details where ptd="{partition}"'''.format(partition=ptd)
        cnt = self.__spark.sql(select_sql).collect()[0][0]
        logger.info('Import {} to hive success, ptd={} has {} data.'.format(tmp_tb_name, ptd, cnt))

    def build_app(self):
        logger.info("Building trip APP ...")
        device_sql = """SELECT bike_id, sum(distance) FROM trip_details WHERE trip_route_type=1 GROUP BY bike_id"""
        self.__spark.sql(device_sql).show()

        logger.info("User behavior analysis ...")
        # by hour
        trip_cnt_by_hour_sql = """
            select 
                start_hour,
                COUNT(1) as used_count,
                SUM(duration) as total_duration,
                SUM(case when plan_duration='1' then 1 else 0 end) as plan_duration_day,
                SUM(case when plan_duration='30' then 1 else 0 end) as plan_duration_month,
                SUM(case when plan_duration='365' then 1 else 0 end) as plan_duration_year,
                SUM(case when trip_route_type='1' then 1 else 0 end) as trip_route_type_one_way,
                SUM(case when trip_route_type='2' then 1 else 0 end) as trip_route_type_round_trip,
                SUM(case when passholder_type='1' then 1 else 0 end) as passholder_type_walk_up,
                SUM(case when passholder_type='2' then 1 else 0 end) as passholder_type_one_day,
                SUM(case when passholder_type='3' then 1 else 0 end) as passholder_type_monthly,
                SUM(case when passholder_type='4' then 1 else 0 end) as passholder_type_annual,
                SUM(case when bike_type='1' then 1 else 0 end) as bike_type_standard,
                SUM(case when bike_type='2' then 1 else 0 end) as bike_type_electric,
                SUM(case when bike_type='3' then 1 else 0 end) as bike_type_smart,
                MAX(case when season='1' then 1 else 0 end) as season_winter,
                MAX(case when season='2' then 1 else 0 end) as season_spring,
                MAX(case when season='3' then 1 else 0 end) as season_summer,
                MAX(case when season='4' then 1 else 0 end) as season_autumn,
                MAX(holiday) as holiday,
                MAX(workingday) as workingday
              from SharedBike.trip_details
              group by start_hour ORDER BY start_hour;
        """
        # """
        #         COUNT(case when holiday=1 then 1 else 0 end) as holiday,
        #         COUNT(case when workingday=1 then 1 else 0 end) as workingday
        # """
        trip_cnt_by_hour_tb_name = 'app_trip_cnt_by_hour'
        df = self.__spark.sql(trip_cnt_by_hour_sql)
        self.__hive.exp_by_tb_name(trip_cnt_by_hour_tb_name, 'results/app/{}'.format(trip_cnt_by_hour_tb_name), df=df)
        logger.info('Generated app_trip_cnt_by_hour.')

    def print_schema(self, df):
        df.printSchema()
        # print(self.__trips_dfs['2021-q1'].schema)
        # print('Tables: {}'.format(spark.catalog.listTables()))
        # print(TRIP_SCHEMA.simpleString())

    def stat_basic(self, df):
        df.describe().show()

    def ctor(self):
        self.__trips_dfs = None
        self.trips_total_df = None


class TripUdf:
    @staticmethod
    def get_date(time_str: str) -> str:
        date_str, time_str = time_str.split(' ')
        return date_str

    @staticmethod
    def get_used_time(time1, time2):
        return int(TimeUtils.duration_by_ts(str(time1), str(time2))/60)

    @staticmethod
    def format_time_to_datetime(time_str: str) -> TimestampType:
        return TimeUtils.string_toDatetime(time_str, format_str="%m/%d/%Y %H:%M", is_check=False)

    @staticmethod
    def format_time_to_hour_str(time_str):
        ts = TimeUtils.string_toDatetime(time_str, format_str="%m/%d/%Y %H:%M", is_check=False)
        return TimeUtils.datetime_toString(ts, format_str='%Y-%m-%d-%H')

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
