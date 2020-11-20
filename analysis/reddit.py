#pip install spark-nlp==1.7.3
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
dataPath = "../data/reddit-worldnews.json"
df = spark.read.json(dataPath)
print(df.count())
df.printSchema()

title = "data.title"
author = "data.author"
dfAuthTitle = df.selectExpr(["data['title']", "data['author']"])
dfAuthTitle.show(5)


#split at spaces, each word exploded into separate arrays,
# now each word is a line,
dfWordCount = df.select(F.explode(F.split(title,"\\s+")).alias("word")
                        .groupBy("word")
                        .count()
                        .orderBy(F.desc("count")))

dfWordCount.show(5)