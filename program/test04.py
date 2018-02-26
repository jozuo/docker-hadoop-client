# encoding=UTF-8
from pyspark import SparkContext, SparkConf
import re


def execute():
    conf = SparkConf().setAppName('test04')
    sc = SparkContext(conf=conf)

    try:
        text_rdd = sc.textFile('/Users/toru/spark/program/data/README.md')
        results = text_rdd \
            .flatMap(lambda value: re.split('[ .,]', value)) \
            .map(lambda value: (value, 1)) \
            .reduceByKey(lambda result, elem: result + elem) \
            .map(lambda value: (value[1], value[0])) \
            .sortByKey(False) \
            .map(lambda value: (value[1], value[0]))

        for result in results.take(3):
            print(result)

    finally:
        sc.stop()


if __name__ == '__main__':
    execute()