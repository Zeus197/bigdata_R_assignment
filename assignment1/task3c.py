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
p1 = allfare1.map(lambda x:(x[0][0],(x[1][0][6],x[1][0][7],x[1][0][8],x[1][0][9])))
p2 = p1.filter(lambda x: (float(x[1][0])==0 and float(x[1][1])==0 and float(x[1][2])==0 and float(x[1][3])==0))
#medallion and the total no of zero gps per medallion
p3 = p2.map(lambda x:((x[0],1))).reduceByKey(add)
p4 = p1.map(lambda x: (x[0],1)).reduceByKey(add)
p5 = p4.leftOuterJoin(p3)
c = p5.map(lambda x:(x[0],x[1][0],int(0 if x[1][1] is None else x[1][1])))
c2 = c.map(lambda x: (x[0],((100*(float(x[2])/float(x[1]))))))
c3 = c2.sortBy(lambda x:(x[0]))
c4 = c3.map(lambda r: ','.join([str(KVPair) for KVPair in r]))
c5= c4.map(lambda r: r.replace("'", ""))
c6 = c5.map(lambda r: r.replace('(', '').replace(')', ''))
c6.saveAsTextFile('task3c.out')