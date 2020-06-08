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

tt88 = "Alltrips"
allfare77.createOrReplaceTempView(tt88)
sql88 = "select medallion,count(*) as zero_gps from Alltrips where pickup_latitude=0.000000 and pickup_longitude=0.000000 and \
 dropoff_latitude=0.000000 and dropoff_longitude=0.000000 group by medallion"
q77 = spark.sql(sql88)

sql89 = "select medallion,count(*) as total_n from Alltrips group by medallion"
q78 = spark.sql(sql89)

tt89 = "zero2"
q77.createOrReplaceTempView(tt89)
tt90 = "total_num"
q78.createOrReplaceTempView(tt90)
sql90 = "select total_num.medallion, total_num.total_n, zero2.zero_gps from total_num left join zero2 on total_num.medallion = zero2.medallion order by total_num.medallion "
q79 = spark.sql(sql90)


tt91 = "total_num1"
q79.createOrReplaceTempView(tt91)
sql91 = "select medallion, total_n, IFNULL(zero_gps, 0) as zero3 from total_num1"
q79 = spark.sql(sql91)

tt92 = "total_num2"
q79.createOrReplaceTempView(tt92)
sql92 = "select medallion, (100* (zero3/total_n)) as percentage from total_num2"
q80 = spark.sql(sql92)

ss2 = q80.rdd.map(tuple)
ss3 = ss2.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
ss4 = ss3.map(lambda r: r.replace("'", ""))
ss5 = ss4.map(lambda r: r.replace('(', '').replace(')', '')) 
ss5.saveAsTextFile('task3c-sql.out')