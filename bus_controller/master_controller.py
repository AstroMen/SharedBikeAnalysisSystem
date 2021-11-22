import sys
from bus_controller.trip_controller import TripController
from statistics_utils.chart_util import ChartUtil
from Logger import logger


class MasterController:
    def __init__(self, spark, hive=None):
        logger.info("Initiating master ...")
        self.__spark = spark
        self.__hive = hive
        self.__is_test = True
        self.__data_folder_name = 'data_test' if self.__is_test else 'data'
        self.__trip_ctl = None

    def init_dw(self):
        self.__spark.sql('create database IF NOT EXISTS SharedBike')
        logger.info('Listing Hive databases ...')
        self.__spark.sql('show databases;').show()
        self.__spark.sql('use SharedBike;')
        logger.info('Listing Hive tables ...')
        self.__spark.sql('show tables;').show()

    def trip_handler(self):
        self.__trip_ctl = TripController(self.__spark, self.__hive, self.__data_folder_name)
        # ODS
        self.__trip_ctl.build_ods()
        self.__trip_ctl.exp_total_to_csv_ods()
        # Middle

        # App


    def statistics(self):
        logger.info('Basic Statistics')
        self.__trip_ctl.stat_basic(self.__trip_ctl.trips_total_df)

        logger.info('Count histogram')
        # col_name = ['trip_route_type', 'passholder_type', 'bike_type', 'season', 'holiday', 'workingday']
        # ChartUtil.gen_histogram(self.__trip_ctl.trips_total_df, n=self.__trip_ctl.trips_total_df.count(), x=col_name)

    def ctor(self):
        self.__trip_ctl.ctor()
        self.__spark.stop()
