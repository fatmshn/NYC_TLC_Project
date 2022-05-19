from pyspark.sql.functions import col, substring, when
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType
from methods import write_to_db, read_data_to_df, get_spark_session, generate_shape_file, generate_fhv_df, \
    generate_fhvhv_df, generate_green_df, generate_yellow_df, parse_arguments
from datetime import datetime,date

arguments = parse_arguments()
date_set =arguments.date
date2 = datetime.strptime(date_set,"%Y-%m" )
date2_m = date2.month
last_month = date2_m - 1 if date2_m != 1 else 12
today_a_month_ago = date(date2.year, last_month, date2.day)
date_premonth = today_a_month_ago.strftime("%Y-%m")

spark = get_spark_session()
df_lookup = read_data_to_df(spark, True, ",", "/Users/fatmik/PycharmProjects/NYC_Taxi_Project/data/taxi+_zone_lookup.csv", "csv")
df_shapefile = generate_shape_file(spark)


df_fhv = generate_fhv_df(spark, date_set)
df_fhv_pre = generate_fhv_df(spark,date_premonth)
df_fhvhv = generate_fhvhv_df(spark, date_set)
df_fhvhv_pre = generate_fhvhv_df(spark,date_premonth)
df_green = generate_green_df(spark, date_set)
df_green_pre = generate_green_df(spark, date_premonth)
df_yellow = generate_yellow_df(spark, date_set)
df_yellow_pre = generate_yellow_df(spark, date_premonth)


dfs = [df_fhv,df_fhvhv,df_green,df_yellow]
df_union = reduce(DataFrame.unionAll, dfs)
df_union = df_union.where("passenger_count!='0'")
df_join_lookup = df_union.join(df_lookup, (df_union.PULocationID == df_lookup.LocationID),"left").select("pickup_datetime","dropoff_datetime","PULocationID","DOLocationID","Borough","Zone","passenger_count").withColumnRenamed("Borough","PU_Borough").withColumnRenamed("Zone","PU_Zone")
df_last = df_join_lookup.join(df_lookup, df_join_lookup.DOLocationID == df_lookup.LocationID,"left" ).withColumnRenamed("Borough", "DO_Borough").withColumnRenamed("Zone", "DO_Zone").drop("service_zone","LocationID")
df_last = df_last.select(substring(col("pickup_datetime"),0,7).alias("month"),col("PULocationID"),col("DOLocationID"),col("PU_Zone"),col("DO_Zone"),col("PU_Borough"),col("DO_Borough"),col("passenger_count")).filter((df_last.PU_Borough!="Unknown")&(df_last.DO_Borough!="Unknown"))
""" Although a single partition is read, it has been observed that data is received for different dates. Filtering was done to generate only the data with the working date."""
df_last = df_last.filter("month=='" + date_set + "'")

dfs_pre = [df_fhv_pre,df_fhvhv_pre,df_green_pre,df_yellow_pre]
df_union_pre = reduce(DataFrame.unionAll, dfs_pre)
df_union_pre = df_union_pre.where("passenger_count!='0'")
df_join_lookup_pre = df_union_pre.join(df_lookup, (df_union_pre.PULocationID == df_lookup.LocationID),"left").select("pickup_datetime","dropoff_datetime","PULocationID","DOLocationID","Borough","Zone","passenger_count").withColumnRenamed("Borough","PU_Borough").withColumnRenamed("Zone","PU_Zone")
df_last_pre = df_join_lookup_pre.join(df_lookup, df_join_lookup_pre.DOLocationID == df_lookup.LocationID,"left" ).withColumnRenamed("Borough", "DO_Borough").withColumnRenamed("Zone", "DO_Zone").drop("service_zone","LocationID")
df_last_pre = df_last_pre.select(substring(col("pickup_datetime"),0,7).alias("month"),col("PULocationID"),col("DOLocationID"),col("PU_Zone"),col("DO_Zone"),col("PU_Borough"),col("DO_Borough"),col("passenger_count")).filter((df_last_pre.PU_Borough!="Unknown")&(df_last_pre.DO_Borough!="Unknown"))
df_last_pre = df_last_pre.filter("month=='" + date_premonth + "'")


""" 2.i : The most popular destinations for each pick-up zone (top-k) in terms of the number of passengers."""
df_last.createOrReplaceTempView("tableA")
spark.sql("SELECT month, PU_Zone,DO_Zone,Pass_Sum, ROW_NUMBER() OVER( PARTITION BY month, PU_Zone ORDER BY Pass_Sum DESC) AS r from (SELECT month, SUM(passenger_count) as Pass_Sum , PU_Zone, DO_Zone from tableA group by month, PU_Zone, DO_Zone order by PU_Zone desc)").filter("r==1").show(100, False)

""" 2.ii :The popularity of destination boroughs for each pick-up borough in terms of number of rides. The borough information can be
found in the previously mentioned lookup table. """
#df_last.createOrReplaceTempView("tableA")
spark.sql("SELECT PU_Borough, DO_Borough,c, ROW_NUMBER() OVER( PARTITION BY PU_Borough ORDER BY c DESC) AS r from (SELECT PU_Borough,DO_Borough,count(*) as c from tableA group by PU_Borough,DO_Borough) ").show(100, False)

""" 3: Create a history of popular rides in the database. For Borough """
df_last_pre.createOrReplaceTempView("tableA_pre")
data_1 = spark.sql("SELECT month, PU_Borough, DO_Borough,c, ROW_NUMBER() OVER( PARTITION BY PU_Borough,month ORDER BY c DESC) AS r from (SELECT month, PU_Borough,DO_Borough,count(*) as c from tableA_pre group by month, PU_Borough,DO_Borough) ")
data_1.createOrReplaceTempView("df1")
data_2 = spark.sql("SELECT month, PU_Borough, DO_Borough,c, ROW_NUMBER() OVER( PARTITION BY PU_Borough,month ORDER BY c DESC) AS r from (SELECT month, PU_Borough,DO_Borough,count(*) as c from tableA group by month, PU_Borough,DO_Borough) ")
data_2.createOrReplaceTempView("df2")
""" Makes a comparison of values with the same pick up point, destination and r = 1 for the working month and the previous month. And it joins the data satisfying these conditions."""
data_bcom = spark.sql("select df2.month, df2.PU_Borough, df2.DO_Borough,df2.c, df2.r from df2 left join df1 on df2.r=df1.r where df2.r=1 and df2.PU_Borough=df1.PU_Borough and df2.DO_Borough=df1.DO_Borough")
data_borough = data_2.subtract(data_bcom).orderBy("PU_Borough","r")
data_borough.show()


""" 3: Create a history of popular rides in the database. For Zone """
data_2 = spark.sql("SELECT month, PU_Zone, DO_Zone,c, ROW_NUMBER() OVER( PARTITION BY PU_Zone,month ORDER BY c DESC) AS r from (SELECT month, PU_Zone,DO_Zone,count(*) as c from tableA group by month, PU_Zone,DO_Zone) ")
data_1 = spark.sql("SELECT month, PU_Zone, DO_Zone,c, ROW_NUMBER() OVER( PARTITION BY PU_Zone,month ORDER BY c DESC) AS r from (SELECT month, PU_Zone,DO_Zone,count(*) as c from tableA_pre group by month, PU_Zone,DO_Zone) ")
data_1.createOrReplaceTempView("df1")
data_2.createOrReplaceTempView("df2")
data_zcom = spark.sql("select df2.month, df2.PU_Zone, df2.DO_Zone,df2.c, df2.r from df2 left join df1 on df2.r=df1.r where df2.r=1 and df2.PU_Zone=df1.PU_Zone and df2.DO_Zone=df1.DO_Zone")
data_zone = data_2.subtract(data_zcom).orderBy("PU_Zone","r")
data_zone.show()

