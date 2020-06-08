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
#fares3.take(10)
#join
allfare = trips3.join(fares3)
allfare1 = allfare.sortByKey(True, 10, keyfunc=lambda k:(k[0], k[1], k[3]))

rev_tol = allfare1.map(lambda x:(x[0][3][0:10],(round(float(x[1][1][1]),2)+round(float(x[1][1][2]),2)+round(float(x[1][1][4]),2),round(float(x[1][1][5]),2))))
rev_tol1 = rev_tol.reduceByKey(lambda x,y: (round(x[0]+y[0],2),round(x[1]+y[1],2)))
rev_tol2 = rev_tol1.sortByKey(lambda x: datetime.datetime.strptime(x[0], '%Y-%m-%d'))
rev_count3 = rev_tol2.map(lambda r: ','.join([str(KVPair) for KVPair in r]))
rev_count4= rev_count3.map(lambda r: r.replace("'", ""))
rev_count5 = rev_count4.map(lambda r: r.replace('(', '').replace(')', '')) 
rev_count6 = rev_count5.map(lambda r: r.replace(" ", "")) 
rev_count6.saveAsTextFile('task2c.out')


