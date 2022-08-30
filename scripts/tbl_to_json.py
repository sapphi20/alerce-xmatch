from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StringType, DoubleType, DecimalType, FloatType, IntegerType
from pyspark.sql.functions import col, concat, struct, lit, array
from pyspark.sql import SparkSession

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

# changed source_id to _id to match mongodb primary key
columns = ['source_name','source_id','ra','dec','sigra','sigdec','sigradec','wx','wy','w1sky','w1sigsk','w1conf','w2sky','w2sigsk','w2conf','w1fitr','w2fitr','w1snr','w2snr','w1flux','w1sigflux','w2flux','w2sigflux','w1mpro','w1sigmpro','w1rchi2','w2mpro','w2sigmpro','w2rchi2','rchi2','nb','na','w1Sat','w2Sat','w1mag','w1sigm','w1flg','w1Cov','w2mag','w2sigm','w2flg','w2Cov','w1mag_1','w1sigm_1','w1flg_1','w2mag_1','w2sigm_1','w2flg_1','w1mag_2','w1sigm_2','w1flg_2','w2mag_2','w2sigm_2','w2flg_2','w1mag_3','w1sigm_3','w1flg_3','w2mag_3','w2sigm_3','w2flg_3','w1mag_4','w1sigm_4','w1flg_4','w2mag_4','w2sigm_4','w2flg_4','w1mag_5','w1sigm_5','w1flg_5','w2mag_5','w2sigm_5','w2flg_5','w1mag_6','w1sigm_6','w1flg_6','w2mag_6','w2sigm_6','w2flg_6','w1mag_7','w1sigm_7','w1flg_7','w2mag_7','w2sigm_7','w2flg_7','w1mag_8','w1sigm_8','w1flg_8','w2mag_8','w2sigm_8','w2flg_8','w1NM','w1M','w1magP','w1sigP1','w1sigP2','w1k','w1Ndf','w1mLQ','w1mJDmin','w1mJDmax','w1mJDmean','w2NM','w2M','w2magP','w2sigP1','w2sigP2','w2k','w2Ndf','w2mLQ','w2mJDmin','w2mJDmax','w2mJDmean','rho12','q12','nIters','nSteps','mdetID','p1','p2','MeanObsMJD','ra_pm','dec_pm','sigra_pm','sigdec_pm','sigradec_pm','PMRA','PMDec','sigPMRA','sigPMDec','w1snr_pm','w2snr_pm','w1flux_pm','w1sigflux_pm','w2flux_pm','w2sigflux_pm','w1mpro_pm','w1sigmpro_pm','w1rchi2_pm','w2mpro_pm','w2sigmpro_pm','w2rchi2_pm','rchi2_pm','pmcode','nIters_pm','nSteps_pm','dist','dw1mag','rch2w1','dw2mag','rch2w2','elon_avg','elonSig','elat_avg','elatSig','Delon','DelonSig','Delat','DelatSig','DelonSNR','DelatSNR','chi2pmra','chi2pmdec','ka','k1','k2','km','par_pm','par_pmSig','par_stat','par_sigma','dist_x','cc_flags','w1cc_map','w1cc_map_str','w2cc_map','w2cc_map_str','n_aw','ab_flags','w1ab_map','w1ab_map_str','w2ab_map','w2ab_map_str','glon','glat','elon','elat','unwise_objid']

spark = SparkSession.builder.appName("DataFrame").config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1').getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile(file_name)

test_rdd = rdd.filter(lambda x: not (x.startswith('|') or x.startswith('\\')))

rdd2 = test_rdd.map(lambda x: x.split())
df = rdd2.toDF(columns)

df2 = df[['source_name', 'source_id', 'ra', 'dec', 'sigra', 'sigdec', 'w1mag', 'w1sigm', 'w1flg', 'w2mag', 'w2sigm', 'w2flg', 'PMRA', 'PMDec', 'sigPMRA', 'sigPMDec', 'w1k', 'w2k', 'w1mlq', 'w2mlq']]

#columns to cast to its respective data type
cols1 = ['sigra', 'sigdec', 'sigPMRA', 'sigPMDEC']
cols2 = ['w1mag', 'w1sigm', 'w2mag', 'w2sigm']
cols3 = ['w1flg',  'w2flg']
cols4 = ['PMRA', 'PMDec']
cols5 = ['w1k', 'w2k']
cols6 = ['w1mlq', 'w2mlq']

obj=df2.withColumnRenamed('source_id', '_id').withColumn("loc", struct(*[lit("Point").alias('type'),array(col("ra").cast(DecimalType(19,7))-180.,col("dec").cast(DecimalType(19,7)))
        .alias('coordinates')])).select('_id', 'source_name', 'loc',*(col(c).cast(IntegerType()) for c in cols3), 
                *(col(c1).cast(DecimalType(13,4)) for c1 in cols1), 
                *(col(c2).cast(DecimalType(10,3)) for c2 in cols2),
                *(col(c4).cast(DecimalType(15,5)) for c4 in cols4),
                *(col(c5).cast(DecimalType(17,5)) for c5 in cols5),
                *(col(c6).cast(DecimalType(8,2)) for c6 in cols6)).drop('ra', 'dec')

obj.write.mode(mode).json(output_path + 'cat')
#obj.write.mode(mode).format('com.mongodb.spark.sql.DefaultSource').option( "uri", "mongodb://127.0.0.1:27017/experiments.objects").save()
