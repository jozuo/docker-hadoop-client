# encoding=UTF-8

from pyspark import SparkContext, SparkConf
from datetime import datetime
import sys


def execute(file_path):
    SUNDAY = 6

    conf = SparkConf().setAppName('Chapter4')
    sc = SparkContext(conf=conf)

    try:
        text_rdd = sc.textFile(file_path)
        datetime_rdd = text_rdd.map(lambda x: datetime.strptime(x, '%Y%m%d'), text_rdd)
        sunday_rdd = datetime_rdd.filter(lambda x: x.weekday() == SUNDAY)
        num_of_sunday = sunday_rdd.count()
        print('与えられたデータの中に日曜日は{}個含まれていました'.format(num_of_sunday))

    finally:
        sc.stop()


def main():
    args = sys.argv

    if len(args) < 2:
        raise Exception('コマンドの引数に日付が記録されたファイルへのパスを入力してください')

    execute(args[1])


if __name__ == '__main__':
    main()
