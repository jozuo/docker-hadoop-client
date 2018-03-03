# endoging=UTF-8

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import IntegerType

conf = SparkConf().setAppName('test06-4')
sc = SparkContext(conf=conf)
context = SQLContext(sc)


def calc_length(str):
    return len(str)


def main():
    # UDF(User Define Function)
    #context.udf.register('strlen', lambda x: len(x), IntegerType())
    context.udf.register('strlen', calc_length, IntegerType())
    print(context.sql('select strlen("Hello World") as strlen').show())


if __name__ == '__main__':
    main()
