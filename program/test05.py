# encoding=UTF-8

from pyspark import SparkContext, SparkConf

FILE_PATH = '/Users/toru/spark/program/data'
MASTER = 'products.csv'
SALES_OCT = 'sales-october.csv'
SALES_NOV = 'sales-november.csv'

conf = SparkConf().setAppName('test05')
sc = SparkContext(conf=conf)


def extract_item_and_sales(line):
    elements = line.split(',')
    product_id = elements[2]
    sales_count = int(elements[3])
    return product_id, sales_count


def load_sales_csv(file):
    return sc.textFile(FILE_PATH + '/' + file) \
        .map(lambda line: extract_item_and_sales(line))


def extract_sales_over_50(sales):
    return sales \
        .reduceByKey(lambda result, element: result + element) \
        .filter(lambda element: element[1] >= 50)


def summary(collection):
    total = 0
    for value in collection:
        if value is not None:
            total += value
    return total


def load_master():
    lines = sc.textFile(FILE_PATH + '/' + MASTER).collect()

    master_list = {}
    for line in lines:
        elements = line.split(',')
        product_id = elements[0]
        product_name = elements[1]
        unit_price = int(elements[2])
        master_list[product_id] = (product_name, unit_price)

    return master_list;


def resolve_master(masters, element):
    product_id = element[0]
    sales_count = element[1]

    master = masters.value[product_id]
    product_name = master[0]
    unit_price = master[1]

    return product_name, sales_count, (sales_count * unit_price)


try:
    # マスターデータをブロードキャスト
    master_map = sc.broadcast(load_master())

    # 売り上げ情報読み込み
    sales_oct = extract_sales_over_50(load_sales_csv(SALES_OCT))
    sales_nov = extract_sales_over_50(load_sales_csv(SALES_NOV))

    # 売り上げ情報結合
    sales_total = sales_oct \
        .fullOuterJoin(sales_nov) \
        .map(lambda element: (element[0], summary(element[1])))

    # マスタ解決
    results = sales_total.map(lambda element: resolve_master(master_map, element))

    # 保存
    results.repartition(1).saveAsTextFile(FILE_PATH + '/' + 'oct-nov-over-50-sold')

finally:
    sc.stop()
