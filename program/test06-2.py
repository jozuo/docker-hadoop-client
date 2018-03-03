# encoding=UTF-8

from pyspark import SparkContext, SparkConf, SQLContext, Row
from pyspark.sql.functions import sum


FILE_PATH = '/Users/toru/spark/program/data'

conf = SparkConf().setAppName('test06-2')
sc = SparkContext(conf=conf)
context = SQLContext(sc)


def get_menu():
    data = sc.textFile(FILE_PATH + '/' + 'dessert-menu.csv').map(lambda l: l.split(','))
    row = data.map(lambda p: Row(menuId=p[0], name=p[1], price=int(p[2]), kcal=int(p[3])))
    df = context.createDataFrame(row)
    df.registerTempTable("dessert_table")
    return df


def get_order():
    data = sc.textFile(FILE_PATH + '/' + 'dessert-order.csv').map(lambda l: l.split(','))
    row = data.map(lambda p: Row(sId=p[0], menuId=p[1], num=int(p[2])))
    return context.createDataFrame(row)


def main(filename):
    tryprice:
        # 結合
        # - 2のDataFrameを用意
        menu = get_menu()
        order = get_order()

        # - menuIdで結合して、メニュー毎の売り上げ金額を導出
        menu_order = menu.join(order, menu.menuId == order.menuId, 'inner') \
            .select(order.sId, menu.name, (order.num * menu.price).alias('price'))
        print(menu_order.show())

        # - 伝票ID(sID)毎の売り上げを導出
        sid_order = menu_order.groupBy('sId').agg(sum('price').alias(('total')))
        print(sid_order.show())

    finally:
        sc.stop()


if __name__ == '__main__':
    main('dessert-menu.csv')
