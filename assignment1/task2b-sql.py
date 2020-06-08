trips_df = spark.read.format('csv').options(header='true', inferschema='true').load("/user/hc2660/hw2data/Trips.csv")
fares_df = spark.read.format('csv').options(header='true', inferschema='true').load("/user/hc2660/hw2data/Fares.csv")
#creating table
temp_table_name = "Trips_csv"                                               
trips_df.createOrReplaceTempView(temp_table_name)
temp_table_name2 = "Fares_csv"
fares_df.createOrReplaceTempView(temp_table_name2)
sql_join = "select Trips_csv.medallion, Trips_csv.hack_license, Trips_csv.vendor_id, \
Trips_csv.pickup_datetime, Trips_csv.rate_code, Trips_csv.store_and_fwd_flag, Trips_csv.dropoff_datetime, \
Trips_csv.passenger_count, Trips_csv.trip_time_in_secs, Trips_csv.trip_distance, Trips_csv.pickup_latitude, Trips_csv.pickup_longitude, \
Trips_csv.dropoff_latitude, Trips_csv.dropoff_longitude, Fares_csv.payment_type, Fares_csv.fare_amount, Fares_csv.surcharge, Fares_csv.mta_tax, Fares_csv.tip_amount, \
Fares_csv.tolls_amount, Fares_csv.total_amount from Trips_csv  inner join Fares_csv on Trips_csv.medallion = Fares_csv.medallion AND Trips_csv.hack_license = Fares_csv.hack_license\
 AND Trips_csv.vendor_id = Fares_csv.vendor_id AND Trips_csv.pickup_datetime = Fares_csv.pickup_datetime order by Trips_csv.medallion, Trips_csv.hack_license, Trips_csv.pickup_datetime "
allfare77 = spark.sql(sql_join)

tt3 = "Alltrips"
allfare77.createOrReplaceTempView(tt3)
sq2 = "select AllTrips.passenger_count from AllTrips"
qe1 = spark.sql(sq2)

tt4 = "p_count"
qe1.createOrReplaceTempView(tt4)
sq4 = "select passenger_count, count(*) as num_trips from p_count group by passenger_count"
qe3 = spark.sql(sq4)

tt5 = "sortedval"
qe3.createOrReplaceTempView(tt5)
sq5 = "select * from sortedval order by passenger_count"
qe4 = spark.sql(sq5)

xx2 = qe4.rdd.map(tuple)
xx3 = xx2.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
xx4= xx3.map(lambda r: r.replace("'", ""))
xx5 = xx4.map(lambda r: r.replace('(', '').replace(')', '')) 
xx6= xx5.map(lambda r: r.replace("-", ","))
xx6.saveAsTextFile('task2b-sql.out')