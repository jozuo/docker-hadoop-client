
from pyspark import SparkContext, SparkConf
from time import sleep

if __name__ == '__main__':
    def heavy_process(element):
        sleep(2)
        return element

    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(10)) \
        .map(heavy_process)

    rdd.cache()

    print('sum: ' + str(rdd.sum()))
    print('count: ' + str(rdd.count()))

    print('finish')
