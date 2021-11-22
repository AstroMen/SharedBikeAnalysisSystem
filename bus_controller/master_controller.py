import sys
from bus_controller.trip_controller import TripController
from statistics_utils.chart_util import ChartUtil
from Logger import logger


class MasterController:
    def __init__(self, spark):
        logger.info("Initiating master ...")
        self.__spark = spark
        self.__is_test = True
        self.__data_folder_name = 'data_test' if self.__is_test else 'data'
        self.__trip_ctl = None

    def trip_handler(self):
        self.__trip_ctl = TripController(self.__spark, self.__data_folder_name)
        self.__trip_ctl.build_rdd()
        self.__trip_ctl.clean_data()
        self.__trip_ctl.exp_total_to_csv()

    def statistics(self):
        logger.info('Basic Statistics')
        self.__trip_ctl.stat_basic(self.__trip_ctl.trips_total_df)

        logger.info('Count histogram')
        col_name = ['trip_route_type', 'passholder_type', 'bike_type', 'season', 'holiday', 'workingday']
        ChartUtil.gen_histogram(self.__trip_ctl.trips_total_df, n=self.__trip_ctl.trips_total_df.count(), x=col_name)

    def ctor(self):
        self.__trip_ctl.ctor()
        self.__spark.stop()
