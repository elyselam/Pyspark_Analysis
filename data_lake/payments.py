from pyspark.sql import SparkSession
import os
import configparser

config = configparser.ConfigParser()

#Normally this file should be in ~/.aws/credentials
config.read_file(open('aws/credentials.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

spark = SparkSession.builder\
                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                     .getOrCreate()


df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv")

df.printSchema()
df.show(5)

df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=";", inferSchema=True, header=True)

df.printSchema()