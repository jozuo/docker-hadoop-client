from datetime import datetime


str = '2018-02-25'
date = datetime.strptime(str, '%Y-%m-%d')
print(date.weekday())
