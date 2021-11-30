import sys
import time, json

from bus_controller.trip_controller import TripController
from statistics_utils.chart_util import ChartUtil
from common.file_utils import FileUtils
from common.geo_utils import GeoUtils
from Logger import logger


class MasterController:
    def __init__(self, spark, hive=None, is_test=True):
        logger.info("Initiating master ...")
        self.__spark = spark
        self.__hive = hive
        self.__is_test = is_test
        self.__data_folder_name = 'data_test' if self.__is_test else 'data'
        self.__trip_ctl = None
        self.__geo_poly_set = dict()
        self.__poly_shape_set = dict()
        self.init_geo_poly()
        # res = GeoUtils.is_exist_in_multi_poly(-118.270813, 34.035679, self.__poly_shape_set['LA'])
        # logger.info(res)

    def init_dw(self):
        self.__spark.sql('create database IF NOT EXISTS SharedBike')
        logger.info('Listing Hive databases ...')
        self.__spark.sql('show databases;').show()
        self.__spark.sql('use SharedBike;')
        logger.info('Listing Hive tables ...')
        self.__spark.sql('show tables;').show()

    def trip_handler(self, re_mid=False):
        self.__trip_ctl = TripController(self.__spark, self.__hive, self.__data_folder_name, self.__geo_poly_set, self.__poly_shape_set)
        # ODS
        # self.__trip_ctl.exp_total_to_csv_ods()

        # Middle
        if re_mid:
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

    def init_geo_poly(self):
        logger.info('Initiating geo poly ...')
        map_poly_path = 'map_poly_json'
        poly_file_name = 'US_LosAngeles_poly'
        file_context = FileUtils.imp_json_file(map_poly_path, poly_file_name, is_abs=True)
        poly_json = json.loads(file_context)
        self.__geo_poly_set['LA'] = poly_json['records'][0]['fields']['geo_shape']  # ['coordinates']
        self.__poly_shape_set['LA'] = GeoUtils.get_poly_shape(self.__geo_poly_set['LA'])

    def ctor(self):
        self.__trip_ctl.ctor()
        self.__spark.stop()
