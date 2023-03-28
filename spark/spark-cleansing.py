import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *

import pandas as pd
import numpy as np

# Create spark session
spark = (SparkSession
    .builder 
    .appName("spark-cleansing") 
    .getOrCreate()
    )
sc = spark.sparkContext
sc.setLogLevel("WARN")

####################################
# Parameters
####################################
csv_file = sys.argv[1]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILE")
print("######################################")

df = (
    spark.read
    .format("csv")
    .option("sep", ",")
    .option("header", True)
    .load(csv_file)
)

####################################
# Format Standarization
####################################
print("######################################")
print("FORMAT STANDARIZATION")
print("######################################")
# Rename education column value from basic.4y, basic.6y, basic.6y into basic
df_transform1 = df.withColumn("YEARS_BIRTH", floor(abs(df["DAYS_BIRTH"] / 365.25))) \
                    .withColumn("YEARS_EMPLOYED", floor(abs(df["DAYS_EMPLOYED"] / 365.25))) \
                    .drop("DAYS_BIRTH") \
                    .drop("DAYS_EMPLOYED")

# Rename education column value from basic.4y, basic.6y, basic.6y into basic
df_transform2 =  df_transform1.withColumn("CODE_GENDER", when(df.CODE_GENDER == "F", 1).otherwise(0)) \
                                    .withColumn("FLAG_OWN_CAR", when(df["FLAG_OWN_CAR"] == "Y", 1).otherwise(0)) \
                                    .withColumn("FLAG_OWN_REALTY", when(df["FLAG_OWN_REALTY"] == "Y", 1).otherwise(0))

####################################
# Cleanse Null Data
####################################
print("######################################")
print("CLEANSE NULL DATA")
print("######################################")
df_transform3 = df_transform2.na.drop("all")

####################################
# Save Data
####################################
print("######################################")
print("SAVE DATA")
print("######################################")
df_transform3.coalesce(1).write \
      .option("header","true") \
      .option("sep",",") \
      .mode("overwrite") \
      .csv("/usr/local/spark/resources/data/spark_output/")

df_transform3.toPandas().to_csv("/usr/local/spark/resources/data/spark_output/applicant_record-full.csv", index=False)  