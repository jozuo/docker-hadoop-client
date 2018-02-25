from pyspark import SparkContext, SparkConf

# PATH_PREFIX='hdfs:///tmp'
PATH_PREFIX = 'file:///Users/toru/spark/program/data'

conf = SparkConf().setAppName('MyFirstStandaloneApp')
sc = SparkContext(conf=conf)

text_file = sc.textFile(PATH_PREFIX + '/shakespeare.txt')
counts = text_file.flatMap(lambda line: line.split(' ')) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)


def printLine(line):
    print(line)


counts.foreach(printLine)
counts.saveAsTextFile(PATH_PREFIX + '/output')
