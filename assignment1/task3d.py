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
dis_med = allfare1.map(lambda x:(x[0][1],x[0][0]))
distinct_val = dis_med.distinct()
distinct_val2 = distinct_val.map(lambda x:(x[0],1)).reduceByKey(add)
p_count1 = distinct_val2.sortBy(lambda x: x[0])
distinct_val3 = p_count1.map(lambda r: ','.join([str(KVPair) for KVPair in r]))
distinct_val4= distinct_val3.map(lambda r: r.replace("'", ""))
distinct_val5 = distinct_val4.map(lambda r: r.replace('(', '').replace(')', '')) 
distinct_val6 = distinct_val5.map(lambda r: r.replace(" ", "")) 
distinct_val6.saveAsTextFile("task3d.out")