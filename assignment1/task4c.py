from csv import reader
from operator import add
import datetime
from datetime import data
#fares dataset
fares_rdd = sc.textFile("/user/hc2660/hw2data/Fares.csv", 1)
fares_rdd = fares_rdd.mapPartitions(lambda x: reader(x))
#license dataset
lic_rdd = sc.textFile("/user/hc2660/hw2data/Licenses.csv", 1)   
lic_rdd = lic_rdd.mapPartitions(lambda x: reader(x))
#lic_rdd.take(10)
header1 = fares_rdd.first()
fares2 = fares_rdd.filter(lambda line: line != header1)

#sort fares
#fares3 = fares2.sortBy(lambda k: k[0], k[1], k[3])
#fares3.take(5)
#removing header
header3 = lic_rdd.first()
lic2 = lic_rdd.filter(lambda line: line != header3)
fares4 = fares2.map(lambda line: ((line[0]), (line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10])))
#sort fares
#fares3 = fares4.sortBy(lambda k: k[0], k[1], k[3])
#fares3.take(5)	
#fares3.take(10)
lic3 = lic2.map(lambda line: ((line[0]), (line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15])))
fr_l = fares4.join(lic3)
fr_l2 = fr_l.sortBy(lambda k:(k[0], k[1][0][0], k[1][0][2]), True, 10)
tot_rev = fr_l2.map(lambda x:(x[1][1][9],round(float(x[1][0][4]),2)))
tot_rev2 = tot_rev.reduceByKey(lambda x,y:(x+y))
tot_rev3 = tot_rev2.sortBy(lambda x:(x[1],x[0]), ascending = False).take(10)
n2 = sc.parallelize([tot_rev3])
tot_rev4 = n2.map(lambda r: ','.join([str(KVPair) for KVPair in r]))
tot_rev5 = tot_rev4.map(lambda r: r.replace("'", ""))
tot_rev6 = tot_rev5.map(lambda r: r.replace('(', '').replace(')', '')) 
tot_rev6.saveAsTextFile('task4c.out')