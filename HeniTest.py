from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initilaise SparkSession

spark = SparkSession \
    .builder \
    .appName("HeniTestApp") \
    .getOrCreate()

data_df = spark.read.format("parquet") \
    .option("header", "True") \
    .load("file:///Users/rkimmoji/Documents/HeniTest/data.parquet")

# Remove duplicates from the data based on the fields: transaction_timestamp,from_address,to_address
data_df_nodups = data_df.dropDuplicates(["transaction_timestamp", "from_address", "to_address"])

# Daily top-5 NFT tokens with the highest amount:
# Rank() the data: Partition by to_date(transaction_timestamp), Order by amount desc
data_df_dailyT5_highest = data_df_nodups.withColumn("date", to_date("transaction_timestamp")) \
    .withColumn("rank", row_number().over(Window.partitionBy("date").orderBy(desc("amount")))) \
    .filter("rank <= 5").select("date", "token_id", "amount")

# Write the above dataframe into an output file
data_df_dailyT5_highest.write.mode("overwrite").format("parquet").option("header", "True") \
    .save("file:///Users/rkimmoji/Documents/HeniTest/Agg_Output1.parquet")

# Daily top-5 NFT tokens with the accumulated transaction amounts:
# Sum(amount): Group by to_date(transaction_timestamp) and token_id
data_df_daily_aggregateAmount = data_df_nodups.withColumn("date", to_date("transaction_timestamp")) \
    .groupBy("date", "token_id") \
    .agg(sum("amount").alias("accumulated_amount"))

# Rank() the data: Partition by to_date(transaction_timestamp), Order by sum(amount) desc
data_df_dailyT5_highestAccumulated = data_df_daily_aggregateAmount \
    .withColumn("rank", row_number().over(Window.partitionBy("date").orderBy(desc("accumulated_amount")))) \
    .filter("rank <= 5").select("date", "token_id", "accumulated_amount")

# Write the above dataframe into another output file
data_df_dailyT5_highestAccumulated.write.mode("overwrite").format("parquet").option("header", "True") \
    .save("file:///Users/rkimmoji/Documents/HeniTest/Agg_Output2.parquet")
