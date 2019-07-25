#----Data pre processing for machine learning-------#
#-----------Import statements-------------#
import pandas as pd
import numpy as np
import pyarrow as pa
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import date_format
import time
#-----------Creating SPARK Session-------------#

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("amazon").config("spark.executor.memory", "6g").config("spark.driver.memory",'6g')
         .getOrCreate())

sc = spark.sparkContext

sqlContext = SQLContext(spark.sparkContext)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

#-----------Reading Data from HDFS-------------#

t1=time.time()
raw_data = spark.read.csv("hdfs://dbda23:9001/input_data/amazon_reviews_us_*",sep ="\t", header = True)

#-----------Data manipulaion in SPARK-------------#
#-----------Removing special charachters and extra columns----------#
#-----------keeping last 2 years data i.e 2014 and 2015----------#
#-----------Filtering data on verified purchase, review length----------#

spark_data = raw_data.drop("customer_id","marketplace","product_parent","vine","product_id","total_votes","helpful_votes")
spark_data = spark_data.where((date_format(spark_data.review_date, 'yyyy') >= '2014')& (col("verified_purchase") == 'Y'))
spark_data = spark_data.drop("verified_purchase","review_date")
spark_data = spark_data.withColumn('review_body', F.regexp_replace('review_body', '[^A-Za-z\s]', ''))
spark_data = spark_data.withColumn('review_headline', F.regexp_replace('review_headline', '[^A-Za-z0-9\s]', ''))
spark_data = spark_data.withColumn('product_title', F.regexp_replace('product_title', '[^A-Za-z0-9\s]', ''))
spark_data = spark_data.withColumn('review_length', F.length('review_body'))
spark_data = spark_data.withColumn('review_head_length', F.length('review_headline'))
spark_data = spark_data.where((col("review_head_length") > 3) & (col("review_length") > 14))
spark_data = spark_data.drop("review_length","review_head_length")

#-----------Defined product categories available in a list----------#

product_list = ['Automotive','Baby','Camera','Digital_Music_Purchase','Digital_Software','Digital_Video_Download','Digital_Video_Games','Electronics','Furniture','Gift Card',
       'Grocery','Home Entertainment','Home Improvement','Jewelry','Lawn and Garden','Luggage','Major Appliances','Mobile_Apps','Mobile_Electronics','Musical Instruments',
       'Office Products','Outdoors','Personal_Care_Appliances','Pet Products','Shoes','Software','Tools','Video Games','Video','Watches']

#-----------Writing seperate datafiles of each category to HDFS------------#

for i in product_list:
    df = spark_data.where(col("product_category") == i)  
    df = df.drop("product_category")
    df.repartition(1).write.save(path="hdfs://dbda23:9001/Machine_Learning_data/"+i,format="csv",header ="true", sep= "\t")
    spark_data = spark_data.where(col("product_category") != i)
    
t2=time.time()
t3=t2-t1
print (t3)
spark.stop()

