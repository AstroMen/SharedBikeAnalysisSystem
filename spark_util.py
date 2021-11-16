import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


java8_location = '/usr/local/Cellar/openjdk@11/11.0.12/libexec/openjdk.jdk/Contents/Home'
os.environ['JAVA_HOME'] = java8_location


class SparkUtil:
    def __init__(self):
        self.__sconf = None
        self.__scontext = None
        self.__ssession = None

    def __build_spark_conf(self, conn="local") -> SparkConf:
        self.__sconf = SparkConf()\
            .set("spark.executor.memory", "2g")\
            .set("spark.driver.allowMultipleContexts", "false")\
            .set("spark.ui.enabled", "true")\
            .setMaster(conn)

    def build_spark_session(self, app_name: str) -> SparkSession:
        self.__build_spark_conf()
        self.__ssession = SparkSession\
            .builder \
            .config(conf=self.__sconf) \
            .appName(app_name) \
            .master("local") \
            .getOrCreate()
            # .enableHiveSupport() \
        return self.__ssession

    def build_spark_context(self, is_local=True, thread=1, is_standalone=False, host=None, port=0) -> SparkContext:
        conn = "local[{}]".format(thread) if is_local and not is_standalone else "spark://{}:{}".format(host, port)
        print(conn)
        self.__build_spark_conf(conn=conn)
        self.__scontext = SparkContext(conf=self.__sconf)
        return self.__scontext

    def stop_session(self):
        self.__ssession.stop()

    def stop_context(self):
        self.__scontext.stop()
