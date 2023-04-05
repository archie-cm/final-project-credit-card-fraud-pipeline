import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *

import pandas as pd
import numpy as np
import pandas_gbq
import os

# Create spark session
spark = (SparkSession
    .builder 
    .appName("spark-cleansing") 
    .getOrCreate()
    )
sc = spark.sparkContext
sc.setLogLevel("WARN")

####################################
# path file
####################################
parquet_file = "/opt/airflow/datasets/application_record.parquet"

####################################
# Read parquet Data
####################################
print("######################################")
print("READING PARQUET FILE")
print("######################################")

df = spark.read.parquet(parquet_file, header=True, inferSchema=True)

####################################
# Format Standarization
####################################
print("######################################")
print("FORMAT STANDARIZATION")
print("######################################")
# Convert days birth & days employed to years and drop columns
df_transform1 = df.withColumn("YEARS_BIRTH", floor(abs(df["DAYS_BIRTH"] / 365.25))) \
                    .withColumn("YEARS_EMPLOYED", floor(abs(df["DAYS_EMPLOYED"] / 365.25))) \
                    .drop("DAYS_BIRTH") \
                    .drop("DAYS_EMPLOYED")

# Convert categorical to numeric in code_gender, flag_own_car and flag_own_realty
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
print("UPLOAD TO BIGQUERY")
print("######################################")

df_transform3_gbq = df_transform3.toPandas()

# TODO: Set project_id to your Google Cloud Platform project ID.
project_id = "data-fellowship-9-project"

# TODO: Set dataset_id to the full destination dataset ID.
table_id = 'final_project.raw_credit_card'

pandas_gbq.to_gbq(df_transform3_gbq, table_id, project_id=project_id)

print("######################################")
print("SUCCESS")
print("######################################")