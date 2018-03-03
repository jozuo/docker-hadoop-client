# encodng=UTF-8

from pyspark import SparkContext, SparkConf, SQLContext, Row

conf = SparkConf().setAppName('test06-1')
sc = SparkContext(conf=conf)
context = SQLContext(sc)


def main(filepath):
    try:
        # ファイル → RDD
        lines = sc.textFile(filepath) \
            .map(lambda line: line.split(','))

        # RDD →　Row
        menus = lines.map(lambda p: Row(
            menuId=p[0], name=p[1], price=int(p[2]), kcal=int(p[3])))

        # Raw → DataFrame
        df = context.createDataFrame(menus)

        # DataFrameを一次テーブルに登録
        df.registerTempTable("dessert_table")
        print("----- table schema -----")
        print(df.printSchema())

        # --------------------------------------------------------------------------------
        # DataFrameへのクエリ発行
        # - SQLキーワードを大文字で記載したサンプルが多いが、小文字でも動作する
        num_over_300kcal = context.sql(
            'select count(*) as num from dessert_table where kcal >= 300')
        print("----- query -----")
        print(num_over_300kcal.show())

        # --------------------------------------------------------------------------------
        # DataFrameAPIでの操作(Select)
        # - カラムの指定
        name_price = df.select(df['name'], df['price'])
        print('----- data frame api (some column) -----')
        print(name_price.show(3))

        # - 全カラム
        name_price = df.select(df['*'])
        print('----- data frame api (all column) -----')
        print(name_price.show(3))

        # - 計算
        name_price = df.select(
            df['name'], (df['price']/120.0).alias('dollar price'))
        print('----- data frame api (all calculate) -----')
        print(name_price.show(3))

        # --------------------------------------------------------------------------------
        # DataFrameAPIでの操作(Filter)
        # - 条件指定
        over_520_yen = df.where(df['price'] >= 520)
        print('----- data frame api (where) -----')
        print(over_520_yen.show())

        # - 条件指定+カラム指定
        # -- selectを先に書くと、whereで絞り込む情報が欠落するので、where → selectの順で指定する
        over_520_yen_name = df.where(df['price'] >= 520).select(df['name'])
        print('----- data frame api (where + select) -----')
        print(over_520_yen_name.show())

        # - ソート
        sorted_result = df.sort(df['price'].asc(), df['kcal'].desc())
        print('----- data frame api (sort) -----')
        print(sorted_result.show())

        # - 集約
        # -- avg, max, min, sum, count が使用可能
        avg_kcal = df.agg({'kcal': 'avg', 'price': 'max'})
        print('----- data frame api (aggregate) -----')
        print(avg_kcal.show())

        # - Groupby
        # -- Python3では`/`の除算は小数点が含まれるようになった。
        # -- 小数点を切り捨てるためには`//`を利用するが、DataFrameAPIでは未サポートの模様
        # -- 小数点を切り捨てるため、`cast('integer')`を利用
        num_per_price_range = df \
            .groupBy(((df['price'] / 100).cast('integer') * 100).alias('price_range')) \
            .agg({'price': 'count'}) \
            .orderBy('price_range', ascending=False)
        print('----- data frame api (group by) -----')
        print(num_per_price_range.show(3))

        # - HiveQL
        print(context.sql('select pi() as PI, e() as E').show())

    finally:
        sc.stop()


if __name__ == '__main__':
    FILE_PATH = '/Users/toru/spark/program/data'
    INPUT_FILE = 'dessert-menu.csv'
    main(FILE_PATH + '/' + INPUT_FILE)
