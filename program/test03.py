# encoding=UTF-8
from pyspark import SparkContext, SparkConf
import re


def is_word(str):
    return re.match(r'^[a-zA-Z0-9]+$', str)


conf = SparkConf().setAppName('test03')
sc = SparkContext(conf=conf)

try:
    text_rdd = sc.textFile('/Users/toru/spark/program/data/simple-words.txt')
    filter_rdd = text_rdd.filter(lambda value: is_word(value))
    map_rdd = filter_rdd.map(lambda value: (value, 1))
    reduce_rdd = map_rdd.reduceByKey(lambda result, element: result + element)
    reduce_rdd.foreach(print)

finally:
    sc.stop()
