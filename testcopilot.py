import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# create a  decorater
def my_decorator(func):
    def wrapper():
        print("Something is happening before the function is called.")
        func()
    return wrapper



# 基数排序算法
def radix_sort(array):
    # 初始化一个长度为10的数组，用来存放每一位的数字
    bucket = [0] * 10
    # 初始化一个长度为10的数组，用来存放每一位的数字
    result = [0] * len(array)
    # 初始化一个变量，用来记录最大的数字的位数
    max_digit = 0
    # 遍历数组，找出最大的数字的位数
    for i in range(len(array)):
        if max_digit < len(str(array[i])):
            max_digit = len(str(array[i]))
    # 循环max_digit次，每次循环都会将数组中的数字按照每一位的数字进行排序
    for i in range(max_digit):
        # 对每一位的数字进行排序
        for j in range(len(array)):
            # 将数组中的数字按照每一位的数字进行排序
            bucket[int(array[j] / (10 ** i) % 10)] += 1
        # 将每一位的数字按照每一位的数字进行排序
        for k in range(1, 10):
            bucket[k] += bucket[k - 1]
        # 将每一位的数字按照每一位的数字进行排序
        for j in range(len(array) - 1, -1, -1):
            result[bucket[int(array[j] / (10 ** i) % 10)] - 1] = array[j]
            bucket[int(array[j] / (10 ** i) % 10)] -= 1
        # 将每一位的数字按照每一位的数字进行排序
        for k in range(len(array)):
            array[k] = result[k]
    return array


# pyspark的应用
def pyspark_app():
    # 创建sparkSession
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
    # 创建sparkContext
    sc = spark.sparkContext
    # 创建rdd
    rdd = sc.parallelize(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])
    # 创建dataframe
    df = spark.createDataFrame(rdd)
    # 打印dataframe
    df.show()
    

pyspark_app()
# print(radix_sort([3,1,5,7,0]))