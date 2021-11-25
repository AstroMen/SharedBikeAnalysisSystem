import sys
import time

from bus_controller.trip_controller import TripController
from statistics_utils.chart_util import ChartUtil
from Logger import logger


class MasterController:
    def __init__(self, spark, hive=None, is_test=True):
        logger.info("Initiating master ...")
        self.__spark = spark
        self.__hive = hive
        self.__is_test = is_test
        self.__data_folder_name = 'data_test' if self.__is_test else 'data'
        self.__trip_ctl = None

    def init_dw(self):
        self.__spark.sql('create database IF NOT EXISTS SharedBike')
        logger.info('Listing Hive databases ...')
        self.__spark.sql('show databases;').show()
        self.__spark.sql('use SharedBike;')
        logger.info('Listing Hive tables ...')
        self.__spark.sql('show tables;').show()

    def trip_handler(self, remid=False):
        self.__trip_ctl = TripController(self.__spark, self.__hive, self.__data_folder_name)
        # ODS
        # self.__trip_ctl.exp_total_to_csv_ods()

        # Middle
        if remid:
            mid_tb_name = 'trip_details'
            self.__hive.drop_tb(mid_tb_name)
            self.__trip_ctl.build_dw()
            self.__hive.exp_by_tb_name(mid_tb_name, 'results/{}'.format(mid_tb_name))

        # App
        self.__trip_ctl.build_app()

    def statistics(self):
        logger.info('Processing statistics ...')
        part_info = self.__hive.get_partition_list('trip_details')

        for year in part_info.collect():
            logger.info('Basic Statistics in {}'.format(year[0]))
            desc_list_names = ['duration', 'plan_duration',  'trip_route_type', 'passholder_type', 'bike_type',
                               'distance', 'season', 'holiday', 'workingday']
            df_year = self.__spark.sql('select {} from trip_details where {};'.format(','.join(desc_list_names), year[0]))
            df_year.describe().show()
            time.sleep(1)

            logger.info('Aggregation Statistics in {}'.format(year[0]))


        # self.__trip_ctl.stat_basic(self.__trip_ctl.trips_total_df)

        logger.info('Count histogram')
        # col_name = ['trip_route_type', 'passholder_type', 'bike_type', 'season', 'holiday', 'workingday']
        # ChartUtil.gen_histogram(self.__trip_ctl.trips_total_df, n=self.__trip_ctl.trips_total_df.count(), x=col_name)

    def ctor(self):
        self.__trip_ctl.ctor()
        self.__spark.stop()
