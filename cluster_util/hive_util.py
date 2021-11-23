from pyspark.sql import HiveContext


class HiveUtil:
    def __init__(self, spark):
        self.__hive_context = None
        self.__spark = spark

    def build_hive_context(self, spark_sess):
        self.__hive_context = HiveContext(spark_sess)

    def exec_sql(self, sql_txt):
        self.__hive_context.sql(sql_txt)

    def show_tb_info(self, tb_name):
        self.__spark.sql('describe formatted {}'.format(tb_name)).show(truncate=False)
        self.__spark.sql('SHOW PARTITIONS {}'.format(tb_name)).show()
        self.__spark.sql('show tables').show()

    def list_tables(self):
        return self.__spark.catalog.listTables()

    def recover_partition(self, tb_name):
        return self.__spark.catalog.recoverPartitions(tb_name)

    def del_partition_by_name(self, tb_name, part_name, part_val):
        self.__spark.sql('alter table {} DROP IF EXISTS PARTITION ({}="{}")'.format(tb_name, part_name, part_val))

    def del_partition_by_condition(self, tb_name, part_cond):
        self.__spark.sql('alter table {} DROP IF EXISTS PARTITION ({}})'.format(tb_name, part_cond))

    def truncate_tb(self, tb_name):
        self.__spark.sql('truncate table {};'.format(tb_name))

    def drop_tb(self, tb_name):
        self.__spark.sql('drop table if exists {};'.format(tb_name))

    def drop_db(self, db_name):
        self.__spark.sql('drop database if exists {};'.format(db_name))

    def exp_by_tb_name(self, tb_name, export_csv_path, partition_cnt=1):
        df = self.__spark.sql('select * from {};'.format(tb_name))
        df.repartition(partition_cnt).write.csv(export_csv_path, encoding="utf-8", header=True)

