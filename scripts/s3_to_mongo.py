from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StringType, DoubleType, DecimalType, FloatType, IntegerType
from pyspark.sql.functions import col, concat, struct, lit, array
from pyspark.sql import SparkSession

from datetime import datetime

import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument('input_path', help='path of the file to be read')
parser.add_argument('-o', '--output', default='/data/json', help='output folder')
parser.add_argument('-b', '--batch-size', default=1000, help='batch size')

args = parser.parse_args()
file_name = args.input_path
output_path = args.output
batch_size = args.batch_size
mode = "overwrite"

spark = SparkSession.builder.appName("DataFrame").config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1').getOrCreate()
sc = spark.sparkContext

#obj.write.mode(mode).json(output_path + 'rej')

start = datetime.now()
#rdd = sc.textFile(file_name)
obj = spark.read.json(file_name)
obj.write.mode(mode).format('com.mongodb.spark.sql.DefaultSource').option( "uri", "mongodb://xmatch-dev:f9oJc5AK8Vdgnb@127.0.0.1:27017/experiments.objects").save()
end = datetime.now()
print("Elapsed time: " + str(end-start))
