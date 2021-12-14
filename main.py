#!/usr/bin/python
from cluster_util.spark_util import SparkUtil
from cluster_util.hive_util import HiveUtil
from bus_controller.master_controller import MasterController
from Logger import logger


__MODULEID__ = "{9690551b-43d2-41d1-96ed-131e9b1aedef}"
__VERSION__ = '1.0.0'
__AUTHOR__ = 'menluyao'
# __all__ = ['method']


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # Initailize spark session
    logger.info('Connecting spark session ...')
    spark = SparkUtil().build_spark_session(app_name="Shared Bike Analysis System")  # is_standalone=True, host="master", port=7077
    hive = HiveUtil(spark)
    # hive = HiveUtil().build_hive_context(spark)
    # if spark is None or hive is None:
    #     logger.error('Cluster connection fail.')
    #     exit(0)

    # Initailize trip
    master = MasterController(spark, hive=hive, is_test=False)
    master.init_dw()
    master.trip_handler(re_mid=True)
    master.statistics()
    master.ctor()
