from csv import reader
from operator import add
import datetime
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
allfare = trips3.join(fares3)
allfare1 = allfare.sortByKey(True, 10, keyfunc=lambda k:(k[0], k[1], k[3]))
dup = allfare1.map(lambda x:(x[0][0],x[0][3]))
dup2 = dup.sortBy(lambda x:(x[0],x[1]))
dup3 = dup2.map(lambda x: ((x[0],x[1]),1)).reduceByKey(add)
dup4 = dup3.filter(lambda x: (x[1]>1))
dup5 = dup4.map(lambda x: (x[0][0],x[0][1]))
dup6 = dup5.sortBy(lambda x:(x[0],x[1]))
dup7 = dup6.map(lambda r: ','.join([str(KVPair) for KVPair in r]))
dup8 = dup7.map(lambda r: r.replace("'", ""))
dup9 = dup8.map(lambda r: r.replace('(', '').replace(')', '')) 
dup9.saveAsTextFile('task3b.out')

#dtime= ['2013-08-06 15:41:00','2013-08-05 18:03:00','2013-08-06 20:55:00','2013-08-07 01:24:00','2013-08-05 09:02:00']
#d2 = sorted(dtime)