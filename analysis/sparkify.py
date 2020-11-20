from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


spark = SparkSession.builder \
        .master("local") \
        .appName("name") \
        .getOrCreate()

user_log = spark.read.json("sparkify_log_small.json")

# user_log.printSchema()
# user_log.show(10)
# user_log.describe().show()
user_log.describe('artist').show()

get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).hour)

udf(lambda x: datetime.datetime.fromtimestamp(x/1000).hour)
user_log = user_log.withColumn("hour", get_hour(user_log.ts))
user_log.show()

songs_in_hour = user_log.filter(user_log.page == "NextSong").groupby(user_log.hour).count().orderBy(user_log.hour.cast("float"))
songs_in_hour.show()

# songs_in_hour_pd = songs_in_hour.toPandas()
# songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)
#
# plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
# plt.xlim(-1, 24);
# plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
# plt.xlabel("Hour")
# plt.ylabel("Songs played");
#
# plt.savefig("yay.png")


# drop rows with missing values, except for ["userId", "sessionId"] columns
user_log_valid = user_log.dropna(how='any', subset = ["userId", "sessionId"])
user_log_valid.count()
user_log.select("userId").dropDuplicates().sort("userId").show()
user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
user_log_valid.count()


#Find users who downgraded their accounts and flag those log entries. Then use a window function and cumulative sum to distinguish each user's data as either pre/post downgrade


user_log_valid.filter("page = 'Submit Downgrade'").show()
user_log.select(["userId", "firstname", "page", "level", "song"]).where(user_log.userId == "1138").collect()

flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))
user_log_valid.head()

from pyspark.sql import Window

windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded").over(windowval))
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log.userId == "1138").sort("ts").collect()