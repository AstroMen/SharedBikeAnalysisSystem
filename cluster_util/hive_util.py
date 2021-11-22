from pyspark.sql import HiveContext


class HiveUtil:
    def __init__(self):
        self.__hive_context = None

    def build_hive_context(self, spark_sess):
        self.__hive_context = HiveContext(spark_sess)

    def exec_sql(self, sql_txt):
        self.__hive_context.sql(sql_txt)
