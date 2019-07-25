#----Data pre processing for Tableau-------#
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

#-----------defining an empty schema -------#
empty_schema = StructType([StructField("product_title",StringType(),True),StructField("product_category",StringType(),True),StructField("number_of_customers",IntegerType(), True)])
category = spark.createDataFrame(sc.emptyRDD(),empty_schema)

#-----------Reading Data from HDFS-------------#

t1=time.time()
raw_data = spark.read.csv("hdfs://dbda23:9001/input_data/amazon_reviews_us_*",sep ="\t", header = True)

#-----------Data manipulaion in SPARK-------------#
#-----------Removing special charachters and extra columns----------#
#-----------keeping last 2 years data i.e 2014 and 2015----------#
#-----------Filtering data on verified purchase, review length----------#

spark_data = raw_data.drop("marketplace","product_parent","vine","product_id","review_headline")
spark_data = spark_data.where(date_format(spark_data.review_date, 'yyyy') >= '2014')
spark_data = spark_data.where(col("verified_purchase") == 'Y')
spark_data = spark_data.drop("verified_purchase")
spark_data = spark_data.withColumn('review_body', F.regexp_replace('review_body', '[^A-Za-z0-9\s]', ''))
spark_data = spark_data.withColumn('product_title', F.regexp_replace('product_title', '[^A-Za-z0-9\s]', ''))
spark_data = spark_data.withColumn('review_length', F.length('review_body'))
spark_data = spark_data.where(col("review_length") > 14)
spark_data = spark_data.drop("review_length")

#-----------Defined product categories available in a list----------#

product_list = ['Automotive','Baby','Camera','Digital_Music_Purchase','Digital_Software','Digital_Video_Download','Digital_Video_Games','Electronics','Furniture','Gift Card',
       'Grocery','Home Entertainment','Home Improvement','Jewelry','Lawn and Garden','Luggage','Major Appliances','Mobile_Apps','Mobile_Electronics','Musical Instruments',
       'Office Products','Outdoors','Personal_Care_Appliances','Pet Products','Shoes','Software','Tools','Video Games','Video','Watches']

#-----------Writing seperate datafiles of each category to HDFS------------#

for i in product_list:
    df = spark_data.where(col("product_category") == i)
    df.repartition(1).write.save(path="hdfs://dbda23:9001/Tab_data/"+i,format="csv",header ="true", sep= "\t")
    spark_data = spark_data.where(col("product_category") != i)

#-----------Number of records in each category---------#    

spark_data.createOrReplaceTempView("spark_data")

volume_data= spark.sql("select product_category,count(review_id) number_of_records from spark_data group by product_category order by count(review_id)")
volume_data.repartition(1).write.save(path="hdfs://dbda23:9001/Tab_data/volume",format="csv",header ="true", sep= "\t")

#------------Sorting top 10 products of each category----------#

for i in product_list:
    df = spark_data.where(col("product_category") == i)
    df.createOrReplaceTempView("category_data")
    select_data = spark.sql("select product_title,product_category,count(customer_id) number_of_customers from category_data group by product_title,product_category order by count(customer_id) desc limit 10")
    category = category.union(select_data)
    spark_data = spark_data.where(col("product_category") != i)
    
#-----------Writing sorted ouput in a file in HDFS----------#    
category.repartition(1).write.save(path="hdfs://dbda23:9001/Tab_data/category",format="csv",header ="true", sep= "\t")

spark.stop()


t2=time.time()
t3=t2-t1
print (t3)