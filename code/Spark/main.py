from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time

spark = SparkSession.builder \
    .appName("StockDataAnalysis") \
    .getOrCreate()
sc = spark.sparkContext

#-------------------------------------------------------------------------------------------------------------
# Read the stock and date
stock_df = spark.read.csv('Fact_Stock_Daily.csv', header=True, inferSchema=True)
date_df = spark.read.csv('Dim_Date_2023.csv', header=True, inferSchema=True)

# Check NULL
stock_null_counts = {col: stock_df.filter(stock_df[col].isNull()).count() for col in stock_df.columns}
date_null_counts = {col: date_df.filter(date_df[col].isNull()).count() for col in date_df.columns}
print(stock_null_counts, date_null_counts)


#-------------------------------------------------------------------------------------------------------------

# Boardcast
# Join date df to stock df without boardcast
start_time = time.time()
joined_df_pre = stock_df.join(date_df, stock_df.Date_ID == date_df.Date_ID)
print(f"Join without boardcast: {time.time() - start_time}")
# Join date df to stock df with boardcast
start_time = time.time()
joined_df_post = stock_df.join(broadcast(date_df), stock_df.Date_ID == date_df.Date_ID)
print(f"Join withboardcast: {time.time() - start_time}")


# Rdd persist
# Average volumn and Amplitude price for each stock without cache
start_time = time.time()
avg_volume_pre = stock_df.groupBy("Stock_ID").avg("Volume")
avg_price_pre = stock_df.groupBy("Stock_ID").avg("Amplitude")
print(f"Without cache: {time.time() - start_time}")
# Average volumn and Amplitude price for each stock with cache
stock_df = stock_df.cache()
start_time = time.time()
avg_volume_pre = stock_df.groupBy("Stock_ID").avg("Volume")
avg_price_pre = stock_df.groupBy("Stock_ID").avg("Amplitude")
print(f"With cache: {time.time() - start_time}")

# Data skew
# Repartition
# total volume for each stock without repartition
start_time = time.time()
total_volume_df_pre = stock_df.groupBy("Stock_ID").sum("Volume")
print(f"Without repartition: {time.time() - start_time}")
# total volume for each stock with repartition
start_time = time.time()
repartitioned_stock_df = stock_df.repartition(4, "Stock_ID")
total_volume_df_post = repartitioned_stock_df.groupBy("Stock_ID").sum("Volume")
print(f"With repartition: {time.time() - start_time}")

# Map-side
stock_rdd = stock_df.rdd.map(lambda row: (row['Stock_ID'], row['Volume']))
start_time = time.time()
grouped = stock_rdd.groupByKey()
average_volumes = grouped.mapValues(lambda volumes: sum(volumes) / len(volumes))
print(f"Without map-side: {time.time() - start_time}")

start_time = time.time()
mapped_rdd = stock_rdd.map(lambda row: (row['Stock_ID'], (row['Volume'], 1)))
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_volumes = reduced_rdd.mapValues(lambda x: x[0] / x[1])

print(f"With map-side: {time.time() - start_time}")

sc.stop()