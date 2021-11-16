# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from spark_util import SparkUtil


__MODULEID__ = "{a75ce56e-c182-5abf-b601-63c19da25db6}"
__VERSION__ = '1.0.0'
__AUTHOR__ = 'menluyao'
# __all__ = ['start_dw_engine']


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    # spark_cont = SparkUtil().build_spark_context()  # is_standalone=True, host="master", port=7077
    # lines = spark_cont.parallelize(["hello world", "hi"])
    # words = lines.flatMap(lambda line: line.split(" "))
    # print(words.count())
    # print(words.first())

    spark = SparkUtil().build_spark_session(app_name="Shared Bike Analysis System")
    db_names = spark.sql("show databases").collect()
    print(db_names)
    tbs_schema = spark.sql("show tables from default;")
    tbs_schema.show()
    print(tbs_schema)

    lines = spark.sparkContext.parallelize(["hello world", "hi"])
    words = lines.flatMap(lambda line: line.split(" "))
    print(words.count())
    print(words.first())

    sum = lines.reduce(lambda x, y: x + y)
    print('元素总和为： ', sum)

    # lines = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
    # sum = lines.reduce(lambda x, y: x + y)
    # print('元素总和为： ', sum)


    # spark_sess.read.options(header='True', inferSchema='True', delimiter=',').csv("/tmp/resources/zipcodes.csv")

    spark.stop()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
