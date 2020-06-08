#Modules to load in dumbo before starting pyspark
 #module load python/gnu/3.6.5
#module load spark/2.4.0
#read csv to dataframe
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
y20 = allfare77.rdd.map(tuple)
 y21 = y20.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
y22= y21.map(lambda r: r.replace("'", ""))
y23 = y22.map(lambda r: r.replace('(', '').replace(')', '')) 
y23.saveAsTextFile('task1a-sql.out')

