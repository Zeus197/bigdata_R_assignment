from csv import reader
from operator import add
#fares dataset
fares_rdd = sc.textFile("/user/hc2660/hw2data/Fares.csv", 1)
fares_rdd = fares_rdd.mapPartitions(lambda x: reader(x))
#fares_rdd.take(10)
#trips dataset
trips_rdd = sc.textFile("/user/hc2660/hw2data/Trips.csv", 1)
trips_rdd = trips_rdd.mapPartitions(lambda x: reader(x))
#trips_rdd.take(10)
#license dataset
lic_rdd = sc.textFile("/user/hc2660/hw2data/Licenses.csv", 1)   
lic_rdd = lic_rdd.mapPartitions(lambda x: reader(x))
#lic_rdd.take(10)
#removing headers
header1 = fares_rdd.first()
fares2 = fares_rdd.filter(lambda line: line != header1)
#fares2.take(10)
header2 = trips_rdd.first()
trips2 = trips_rdd.filter(lambda line: line != header2)
#trips2.take(10)
header3 = lic_rdd.first()
lic2 = lic_rdd.filter(lambda line: line != header3)
#lic2.take(10)
#mapping(k,v)
trips3 = trips2.map(lambda line : ((line[0],line[1], line[2], line[5]), (line[3], line[4], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13]))) 
#trips3.take(10)
fares3 = fares2.map(lambda line : ((line[0], line[1], line[2], line[3]), (line[4], line[5], line[6], line[7], line[8], line[9], line[10])))
#fares3.take(10)
#join
allfare = trips3.join(fares3)
allfare1 = allfare.sortByKey(True, 10, keyfunc=lambda k:(k[0], k[1], k[3]))

p_c = allfare1.map(lambda x: (x[1][0][3]))
p_c2 = p_c.map(lambda x: (x,1)).reduceByKey(add)
pc3 = p_c2.sortBy(lambda x: (x[0]))
p4 = pc3.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
p5= p4.map(lambda r: r.replace("'", ""))
p6 = p5.map(lambda r: r.replace('(', '').replace(')', '')) 
p6.saveAsTextFile('task2b.out')