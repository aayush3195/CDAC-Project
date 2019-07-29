#----Data pre processing for Tableau-------#
#-----------Import statements-------------#
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

#-----------defining an empty schema -------#

empty_schema = StructType([StructField("customer_id",StringType(),True),
                           StructField("review_id",StringType(),True),
                           StructField("product_title",StringType(), True),
                           StructField("product_category",StringType(), True),
                           StructField("star_rating",StringType(),True),
                           StructField("helpful_votes",StringType(),True),
                           StructField("total_votes",StringType(), True),
                           StructField("review_body",StringType(), True),
                           StructField("review_date",StringType(), True)])
category = spark.createDataFrame(sc.emptyRDD(),empty_schema)

empty_schema1 = StructType([StructField("product_category",StringType(),True),
                           StructField("number_of_records",IntegerType(),True)])
volume = spark.createDataFrame(sc.emptyRDD(),empty_schema1)
#-----------Reading Data from HDFS-------------#

t1=time.time()
input_data = ['Automotive','Baby','Camera','Digital_Music_Purchase','Digital_Software','Digital_Video_Download','Digital_Video_Games','Electronics','Furniture','Gift_Card',
       'Grocery','Home_Entertainment','Home_Improvement','Jewelry','Lawn_and_Garden','Luggage','Major_Appliances','Mobile_Apps','Mobile_Electronics','Musical_Instruments',
       'Office_Products','Outdoors','Personal_Care_Appliances','Pet_Products','Shoes','Software','Tools','Video_Games','Video','Watches']


for i in input_data:  
    raw_data = spark.read.csv("hdfs://dbda23:9001/input_data/amazon_reviews_us_"+i+"_v1_00.tsv",sep ="\t", header = True)

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
    spark_data.repartition(1).write.save(path="hdfs://dbda23:9001/Tableau_categorywise_files/"+i,format="csv",header ="true", sep= ",")
    spark_data.createOrReplaceTempView("category_data")
    select_data = spark.sql("select * from category_data where product_title in(select product_title from category_data group by product_title order by count(customer_id) desc limit 10)")
    category = category.union(select_data)
    data_volume = spark.sql("select product_category,count(review_id) number_of_records from category_data group by product_category order by count(review_id)")
    volume = volume.union(data_volume)
category.repartition(1).write.save(path="hdfs://dbda23:9001/Tableau_top_10_each_category/",format="csv",header ="true", sep= ",")
volume.repartition(1).write.save(path="hdfs://dbda23:9001/Tableau_data_volume",format="csv",header ="true", sep= ",")

t2=time.time()
t3=t2-t1
print (t3)
spark.stop()