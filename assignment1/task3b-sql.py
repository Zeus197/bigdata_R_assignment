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

tt33 = "Alltrips"
allfare77.createOrReplaceTempView(tt33)
sql22 = "select Alltrips.medallion, Alltrips.pickup_datetime from Alltrips"
q22 = spark.sql(sql22)

tt34 = "duplicate"
q22.createOrReplaceTempView(tt34)
sql23 = "select medallion, pickup_datetime, count(*) as count1 from duplicate group by medallion, pickup_datetime having count1>1 order by medallion, pickup_datetime "
q23 = spark.sql(sql23)


tt35 = "duplicates"
q23.createOrReplaceTempView(tt35)
sql24 = "select medallion, pickup_datetime from duplicates "
q24 = spark.sql(sql24)

pp2 = q24.rdd.map(tuple)
pp3 = pp2.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
pp4= pp3.map(lambda r: r.replace("'", ""))
pp5 = pp4.map(lambda r: r.replace('(', '').replace(')', '')) 
pp5.saveAsTextFile('task3b-sql.out')