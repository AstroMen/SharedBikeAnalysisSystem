# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import re
from spark_util.spark_util import SparkUtil
from bus_controller.master_controller import MasterController
from Logger import logger


__MODULEID__ = "{9690551b-43d2-41d1-96ed-131e9b1aedef}"
__VERSION__ = '1.0.0'
__AUTHOR__ = 'menluyao'
# __all__ = ['start_dw_engine']


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # Initailize spark session
    logger.info('Connecting spark session ...')
    spark = SparkUtil().build_spark_session(app_name="Shared Bike Analysis System")  # is_standalone=True, host="master", port=7077

    # Initailize trip
    master = MasterController(spark)
    master.trip_handler()
    master.statistics()

    # db_names = spark.sql("show databases").collect()
    # tbs_schema = spark.sql("show tables from default;")
    # tbs_schema.show()
    #
    # lines = spark.sparkContext.parallelize(["hello world", "hi"])
    # words = lines.flatMap(lambda line: line.split(" "))
    # print(words.count())
    # print(words.first())
    #
    # sum = lines.reduce(lambda x, y: x + y)
    # print('元素总和为： ', sum)

    master.ctor()
