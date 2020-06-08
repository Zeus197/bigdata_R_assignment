fares_df = spark.read.format('csv').options(header='true', inferschema='true').load("/user/hc2660/hw2data/Fares.csv")
lic_df = spark.read.format('csv').options(header='true', inferschema='true').load("/user/hc2660/hw2data/Licenses.csv")

temp_table_name = "Lic_csv"                                               
lic_df.createOrReplaceTempView(temp_table_name)
temp_table_name2 = "Fares_csv"
fares_df.createOrReplaceTempView(temp_table_name2)
sql_join = "select Fares_csv.medallion, Fares_csv.hack_license, Fares_csv.vendor_id, Fares_csv.pickup_datetime, Fares_csv.payment_type, Fares_csv.fare_amount,\
 Fares_csv.surcharge, Fares_csv.mta_tax, Fares_csv.tip_amount, Fares_csv.tolls_amount, Fares_csv.total_amount, Lic_csv.name, Lic_csv.type, Lic_csv.current_status, \
 Lic_csv.DMV_license_plate, Lic_csv.vehicle_VIN_number, Lic_csv.vehicle_type, Lic_csv.model_year, Lic_csv.medallion_type, Lic_csv.agent_number, Lic_csv.agent_name, \
 Lic_csv.agent_telephone_number, Lic_csv.agent_website, Lic_csv.agent_address, Lic_csv.last_updated_date, Lic_csv.last_updated_time from Fares_csv  \
 inner join Lic_csv on Fares_csv.medallion = Lic_csv.medallion order by Fares_csv.medallion, Fares_csv.hack_license, Fares_csv.pickup_datetime "
f_lic = spark.sql(sql_join)

tt55 = "fare_lic"
f_lic.createOrReplaceTempView(tt55)
sql44 = "select vehicle_type, count(*) as total_trips, sum(fare_amount) as total_revenue,\
 ((100/count(*)) * sum(fare_amount/tip_amount)) as avg_percentage from fare_lic where tip_amount <> 0 group by vehicle_type order by vehicle_type"
cc = spark.sql(sql44)

dd2 = cc.rdd.map(tuple)
dd3 = dd2.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
dd4= dd3.map(lambda r: r.replace("'", ""))
dd5 = dd4.map(lambda r: r.replace('(', '').replace(')', '')) 
dd5.saveAsTextFile('task4a-sql.out')

