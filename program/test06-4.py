# encoding=UTF-8
# 構造化データセットの扱い

from pyspark import SparkContext, SparkConf, SQLContext, Row


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


def write_and_read(format):
    # Python3ではSaveModeの定数定義されたクラスは無さそう
    SAVE_MODE_ERROR_IF_EXISTS = 'error'
    SAVE_MODE_OVERWRITE = 'overwrite'
    SAVE_MODE_APPEND = 'append'
    SAVE_MODE_IGNORE = 'ignore'  # 既存データが存在する場合 何もしない

    # DataFrameの生成
    df = get_menu()

    # 出力
    # SaveMode
    df_writer = df.write
    df_writer.format(format).mode(SAVE_MODE_IGNORE).save('file:///tmp/spark/' + format)

    # 入力
    df_reader = context.read
    df2 = df_reader.format(format).load('file:///tmp/spark/' + format)
    print(df2.orderBy('name').show(3))


if __name__ == '__main__':
    try:
        # Parquet: Apache Parquet形式。大規模データの分析に適した(列指向の)データフォーマット
        write_and_read('parquet')
        # Json
        write_and_read('json')
        # ORC: Hiveの処理に最適化された列指向のファイルフォーマット
        write_and_read('orc')

        print('----- table -----')
        df = get_menu()
        df.write.format('parquet').saveAsTable("tbl_parquet")
        print(context.read.format('parquet').table('tbl_parquet').show(3))

    finally:
        print('finish')
        sc.stop()
