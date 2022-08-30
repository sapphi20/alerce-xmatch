from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, struct, lit, array
import sys
import time

output_dir = sys.argv[1]
min_parquet = sys.argv[2]
parquet_num = sys.argv[3]
maxRecordsPerFile = 100000
mode = "overwrite"

#conf
conf = SparkConf()
spark = SparkSession.builder.config(conf=conf).getOrCreate()
#sc = SparkContext(conf=conf)
#sc.setLogLevel('WARN')
#sc.batchSize = 1000

#read dataframe
#dfo = spark.read.load("s3a://ztf-dr3/object/")
#dfd = spark.read.load("s3a://ztf-dr3/detection/*")


for i in range(int(min_parquet), int(parquet_num)):
    print("Iteration n°: " + str(i+1))
    df = spark.read.load("s3a://allwise/zone=" + str(i + 1) +  "/*.parquet")
    obj=df.withColumn("_id",col("source_id").cast(StringType())).withColumn("loc", struct(*[lit("Point").alias('type'),array(col("ra")-180.,col("dec"))
        .alias('coordinates')])).select("_id","loc")
    obj.write.mode(mode).json(output_dir + "allwiseSample" + str(i + 1))

#rdd = sc.textFile("s3a://allwise/zone=12*")
#print(rdd.count())

#rdd.foreachPartition(process_batch)

#tt_obj = dfo.filter((dfo.oid<245106200000010) & (dfo.oid>=245106200000000))
#tt_det = dfd.filter((dfd.oid<245106200000010) & (dfd.oid>=245106200000000))
#tt_tot = dfo.filter((dfo.oid<245106200000010) & (dfo.oid>=245106200000000)).alias('o').join(dfd.alias('d'), 'oid')

# RA must have values between -180 and 180


#det=dfd.withColumn("source_id",col("source_id").cast(StringType()))

#obj.write.mode(mode).json(output_dir + "dr3SampleObj")
#det.write.mode(mode).json(output_dir + "dr3SampleDet")

#tt_tot.coalesce(n_partitions).write.option("maxRecordsPerFile", maxRecordsPerFile).mode(mode).json(output_dir + "dr3SampleObjDet")
#s3-dist-cp --src /incoming/hourly_table_filtered --dest s3://my-tables/incoming/hourly_table_gz --outputCodec=gz
