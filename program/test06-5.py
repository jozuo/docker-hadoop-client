# encoding=UTF-8
# パーティショニング

from pyspark import SparkContext, SparkConf, SQLContext, Row

# pylint: disable=E0611
from pyspark.sql.functions import sum
# pylint: enable=E0611


FILE_PATH = 'file:///Users/toru/spark/program/data'

conf = SparkConf().setAppName('test06-2')
sc = SparkContext(conf=conf)
context = SQLContext(sc)


def get_menu():
    data = sc.textFile(FILE_PATH + '/' + 'dessert-menu.csv').map(lambda l: l.split(','))
    row = data.map(lambda p: Row(menuId=p[0], name=p[1], price=int(p[2]), kcal=int(p[3])))
    df = context.createDataFrame(row)
    df.registerTempTable("dessert_table")
    return df


if __name__ == '__main__':
    try:
        df = get_menu()
        price_range_df = \
            df.select(((df['price'] / 100).cast('integer') * 100).alias('price_range'), df['*'])

        # 出力
        # - パーティショニング:なし
        price_range_df.write.format('json').mode('overwrite').save('/tmp/spark/non_partitioned')
        # - パーティショニング:あり
        price_range_df.write.format('json').partitionBy('price_range').mode('overwrite').save('/tmp/spark/partitioned')

        # 読み込み
        non_partitioned_df = context.read.format('json').load('/tmp/spark/non_partitioned')
        partitioned_df = context.read.format('json').load('/tmp/spark/partitioned')

        # 実行計画出力
        # - パーティショニング:なし → 実行計画にフィルタリングが含まれる
        print('----- non partitioned -----')
        non_partitioned_df.where(non_partitioned_df['price_range'] >= 500).explain()

        # - パーティショニング:あり → 実行計画にフィルタリングが含まれない
        print('----- partitioned -----')
        partitioned_df.where(partitioned_df['price_range'] >= 500).explain()

    finally:
        print('finish')
        sc.stop()
