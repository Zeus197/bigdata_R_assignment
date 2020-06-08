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
z20 = f_lic.rdd.map(tuple)
z21 = z20.map(lambda r: ','.join([str(KVPair) for KVPair in r])) 
z22= z21.map(lambda r: r.replace("'", ""))
z23 = z22.map(lambda r: r.replace('(', '').replace(')', '')) 
z23.saveAsTextFile('task1b-sql.out')