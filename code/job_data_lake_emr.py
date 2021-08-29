#spark-submit s3://csancho-datalake-emr-script/dojo-data/script/job_data_lake_emr.py s3://csancho-datalake-emr-script/dojo-data/input/customers.csv s3://csancho-datalake-emr-script/dojo-data/output/taskoutput/

import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .appName("SparkETL")\
    .getOrCreate()

#customerdf = spark.read.option("inferSchema", "true").option("header", "true")\
#        .csv("s3://csancho-datalake-emr-script/dojo-data/input/customers.csv")

customerdf = spark.read.option("inferSchema", "true").option("header", "true").csv(sys.argv[1])

customerdf.printSchema()

customerdf = customerdf.select("CUSTOMERNAME","EMAIL")
customerdf.printSchema()

#customerdf.write.format("parquet").mode("overwrite").save("s3://csancho-datalake-emr-script/dojo-data/output/")
customerdf.write.format("parquet").mode("overwrite").save(sys.argv[2])

spark.catalog.setCurrentDatabase("csancho_datalake_raw_dev")
df = spark.sql("select * from niv_niv3_flatfile")
df.show()

df = df.select("CUSTOMERNAME","EMAIL")
df.show()

df.write.format("json").mode("overwrite").save("s3://csancho-datalake-emr-script/dojo-lake/output/niv_niv3/")
