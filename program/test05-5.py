# encoding=UTF-8

from pyspark import SparkContext, SparkConf

FILE_PATH = '/Users/toru/spark/program/data'
INPUT_FILE = 'questionnaire.csv'


conf = SparkConf().setAppName('test05-5')
sc = SparkContext(conf=conf)

num_m_acc = sc.accumulator(0)
num_f_acc = sc.accumulator(0)
total_m_acc = sc.accumulator(0)
total_f_acc = sc.accumulator(0)


# 代入する場合は 変数を`global`で定義し直す必要あり
# def calc(element):
#     global num_m_acc
#     global num_f_acc
#     global total_m_acc
#     global total_f_acc
#
#     if element[1] == 'M':
#         num_m_acc += 1
#         total_m_acc += element[2]
#     else:
#         num_f_acc += 1
#         total_f_acc += element[2]

# メソッド呼び出しの場合は問題なし
def calc(element):
    if element[1] == 'M':
        num_m_acc.add(1)
        total_m_acc.add(element[2])
    else:
        num_f_acc.add(1)
        total_f_acc.add(element[2])


def split_data(line):
    array = line.split(',')
    age_range = int(int(array[0]) / 10) * 10  # 年代に変換
    sex = array[1]
    point = int(array[2])
    return age_range, sex, point


try:
    # データの読み込み
    data = sc.textFile(FILE_PATH + '/' + INPUT_FILE) \
        .map(split_data)

    # データを永続化(MEMORY_ONLY)
    data.cache()

    # 全体平均の算出1
    # count = data.count()
    # total_point = data.map(lambda element: element[2]).sum()
    # print('AVG ALL: ' + str(total_point / count))

    # 全体平均の算出2
    total_average = data\
        .map(lambda element: (element[2], 1)) \
        .reduce(lambda result, element: (result[0] + element[0], result[1] + element[1]))
    print('AVG ALL: ' + str(total_average[0] / total_average[1]))

    # 年代別平均
    age_range_average = data.map(lambda element: (element[0], (element[2], 1))) \
        .reduceByKey(lambda result, element: (result[0] + element[0], result[1] + element[1]))

    for value in age_range_average.collect():
        print('AVG Age Range(' + str(value[0]) + '): ' + str(value[1][0] / value[1][1]))

    # 性別毎平均
    # アキュームレータを利用する
    data.foreach(calc)
    print('AVG M: ' + str(total_m_acc.value / num_m_acc.value))
    print('AVG F: ' + str(total_f_acc.value / num_f_acc.value))

finally:
    sc.stop()
