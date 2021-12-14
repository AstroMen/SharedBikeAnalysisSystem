import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from common.Logger import logger


java8_location = '/usr/local/Cellar/openjdk@11/11.0.12/libexec/openjdk.jdk/Contents/Home'
os.environ['JAVA_HOME'] = java8_location


class SparkUtil:
    def __init__(self):
        self.__sconf = None
        self.__scontext = None
        self.__ssession = None

    def __build_spark_conf(self, conn="local"):
        self.__sconf = SparkConf()\
            .set("spark.executor.memory", "2g")\
            .set("spark.driver.allowMultipleContexts", "false")\
            .set("spark.ui.enabled", "true")\
            .setMaster(conn)

    def build_spark_session(self, app_name):
        logger.info('Build spark session ...')
        self.__build_spark_conf()
        self.__ssession = SparkSession.builder\
            .config(conf=self.__sconf) \
            .appName(app_name) \
            .master("local[*]") \
            .enableHiveSupport() \
            .getOrCreate()
        self.__ssession.sparkContext.setLogLevel("ERROR")
        return self.__ssession

    def build_spark_context(self, is_local=True, thread=1, is_standalone=False, host=None, port=0):
        logger.info('Build spark context ...')
        conn = "local[{}]".format(thread) if is_local and not is_standalone else "spark://{}:{}".format(host, port)
        print(conn)
        self.__build_spark_conf(conn=conn)
        self.__scontext = SparkContext(conf=self.__sconf)
        return self.__scontext

    def get_tb_from_mysql(self, ip, db_name, tb_name, user_name, pwd, port=3306):
        if self.__ssession is None:
            logger.warn('get_tb_from_mysql: session does not exist.')
            return None
        df_jdbc = self.__ssession.read.jdbc(url="jdbc:mysql://{}:{}/{}".format(ip, port, db_name),
                                  table=tb_name, properties={'user': user_name, 'password': pwd})
        return df_jdbc

    def stop_session(self):
        self.__ssession.stop()

    def stop_context(self):
        self.__scontext.stop()
