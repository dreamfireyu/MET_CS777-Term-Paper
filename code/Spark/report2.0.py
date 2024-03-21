from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Spark Session initialization
spark = SparkSession.builder.appName("DynamicDateReport").getOrCreate()

# Load datasets
stock_df = spark.read.csv('Fact_Stock_Daily.csv', header=True, inferSchema=True)
date_df = spark.read.csv('Dim_Date_2023.csv', header=True, inferSchema=True)

# Input info
input_year = 2023
input_month = 1
input_day = 11
input_quarter = None

start_time = time.time()
# Join date with stock
joined_df = stock_df.join(date_df, "Date_ID")

# Type of report
if input_quarter:
    # Specific quarter
    filtered_df = joined_df.filter((col('Year') == input_year) & (col('Quarter') == input_quarter))
elif input_day:
    # Specific day
    filtered_df = joined_df.filter((col('Year') == input_year) & (col('Month') == input_month) & (col('Day') == input_day))
elif input_month:
    # Specific month
    filtered_df = joined_df.filter((col('Year') == input_year) & (col('Month') == input_month))
else:
    # Specific year
    filtered_df = joined_df.filter(col('Year') == input_year)
    
# Top 10 Stocks by Max Volume
top_volume_stocks = filtered_df.groupBy("Stock_ID").agg({"Volume": "max"}).withColumnRenamed("max(Volume)", "MaxVolume").orderBy(col("MaxVolume").desc()).limit(10)

# Top 10 Stocks by Max Amplitude
top_amplitude_stocks = filtered_df.groupBy("Stock_ID").agg({"Amplitude": "max"}).withColumnRenamed("max(Amplitude)", "MaxAmplitude").orderBy(col("MaxAmplitude").desc()).limit(10)

# Top 10 Stocks by Increase Percent
top_percent_increase_stocks = filtered_df.orderBy(col("Percent_Of_Incre_Decre").desc()).select("Stock_ID", "Percent_Of_Incre_Decre").limit(10)

# Top 10 Stocks by Decrease Percent
top_percent_decrease_stocks = filtered_df.orderBy(col("Percent_Of_Incre_Decre").asc()).select("Stock_ID", "Percent_Of_Incre_Decre").limit(10)

# Top 10 Stocks by Increase Amount
top_amount_increase_stocks = filtered_df.orderBy(col("Amount_Of_Incre_Decre").desc()).select("Stock_ID", "Amount_Of_Incre_Decre").limit(10)

# Top 10 Stocks by Decrease Amount
top_amount_decrease_stocks = filtered_df.orderBy(col("Amount_Of_Incre_Decre").asc()).select("Stock_ID", "Amount_Of_Incre_Decre").limit(10)

# Show Result
if input_quarter:
    rpt = f'{input_year}/Q{input_quarter}'
elif input_day:
    rpt = f'{input_year}/{input_month}/{input_day}'
elif input_month:
    rpt = f'{input_year}/{input_month}'
else:
    rpt = input_year

print(f"Top 10 Stocks by Max Volume for {rpt}:")
top_volume_stocks.show()

print(f"Top 10 Stocks by Max Amplitude for {rpt}:")
top_amplitude_stocks.show()

print(f"Top 10 Stocks by Increase Percent for {rpt}:")
top_percent_increase_stocks.show()

print(f"Top 10 Stocks by Decrease Percent for {rpt}:")
top_percent_decrease_stocks.show()

print(f"Top 10 Stocks by Increase Amount for {rpt}:")
top_amount_increase_stocks.show()

print(f"Top 10 Stocks by Decrease Amount for {rpt}:")
top_amount_decrease_stocks.show()

print(f"Time used: {time.time() - start_time}")


spark.stop()