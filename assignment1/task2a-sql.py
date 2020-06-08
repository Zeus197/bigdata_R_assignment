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
temp_table_name3 = "Alltrips"
allfare77.createOrReplaceTempView(temp_table_name3)
sql_q2 = "select AllTrips.fare_amount from AllTrips"
qq1 = spark.sql(sql_q2)
t4 = "fare_amt"
qq1.createOrReplaceTempView(t4)
sql_3 = "select fare_amount, case when fare_amount >=0 and fare_amount <=5 then '0-5' when fare_amount >5 and fare_amount <=15 then '5-15' when fare_amount >15 and fare_amount <=30 then '15-30' when fare_amount >30 and fare_amount <=50 then '30-50' when fare_amount >50 and fare_amount <=100 then '50-100' when fare_amount >100 then '>100' else '<0' end as amount_range from fare_amt"
qq2 = spark.sql(sql_3)
t5 = "amt_range"
qq2.createOrReplaceTempView(t5)
sql_4 = "select amount_range, count(*) as num_trips from amt_range group by amount_range"
qq3 = spark.sql(sql_4)
t6 = "amt_ran"
qq3.createOrReplaceTempView(t6)
sql_5 = "select * from amt_ran where amount_range <> '<0' order by amount_range"
qq4 = spark.sql(sql_5)
x3 = qq4.rdd.map(tuple)
x21 = x3.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
x22= x21.map(lambda r: r.replace("'", ""))
x23 = x22.map(lambda r: r.replace('(', '').replace(')', '')) 
x24= x23.map(lambda r: r.replace("-", ","))
x24.saveAsTextFile('task2a-sql.out')
