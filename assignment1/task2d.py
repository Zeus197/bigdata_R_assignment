from csv import reader
from operator import add
import datetime
from datetime import date
from datetime import timedelta
from datetime import datetime
#fares dataset
fares_rdd = sc.textFile("/user/hc2660/hw2data/Fares.csv", 1)
fares_rdd = fares_rdd.mapPartitions(lambda x: reader(x))
#fares_rdd.take(10)
#trips dataset
trips_rdd = sc.textFile("/user/hc2660/hw2data/Trips.csv", 1)
trips_rdd = trips_rdd.mapPartitions(lambda x: reader(x))
#trips_rdd.take(10)
#license dataset
header1 = fares_rdd.first()
fares2 = fares_rdd.filter(lambda line: line != header1)
#fares2.take(10)
header2 = trips_rdd.first()
trips2 = trips_rdd.filter(lambda line: line != header2)

trips3 = trips2.map(lambda line : ((line[0],line[1], line[2], line[5]), (line[3], line[4], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13]))) 
#trips3.take(10)
fares3 = fares2.map(lambda line : ((line[0], line[1], line[2], line[3]), (line[4], line[5], line[6], line[7], line[8], line[9], line[10])))
#fares3.take(10)
#join
allfare = trips3.join(fares3)
allfare1 = allfare.sortByKey(True, 10, keyfunc=lambda k:(k[0], k[1], k[3]))

days_d = allfare1.map(lambda x: (x[0][0], x[0][3][0:10], x[1][0][2][0:10]))





def format_date(x):
	return datetime.strptime(x, '%Y-%m-%d')


# days_d2 = days_d.map(lambda x: (x[0] + ","+ x[1] +','+x[2],1))
days_d3 = days_d.flatMap(lambda x: [str(x[0]) + "-" + str((format_date(x[1]) + timedelta(y)).date()) for y in range((format_date(x[2]) - format_date(x[1])).days + 1)])
day4 = days_d3.map(lambda x:(x,1)).reduceByKey(add)
days5 = day4.map(lambda x:(x[0][:32],(x[0][33:43],x[1])))
days6 = days5.map(lambda x:(x[0],x[1][1])).reduceByKey(lambda x,y:(x+y))
days7 = days5.map(lambda x:(x[0],(x[1][0],1))).reduceByKey(add)
days_d3 = days_d2.reduceByKey(lambda x,y :(x[0]+y[0],x[1]+y[1]))
days_d4 = days_d3.map(lambda x: (x[0],x[1][0],x[1][1],round(float(x[1][0]/x[1][1]),2)))