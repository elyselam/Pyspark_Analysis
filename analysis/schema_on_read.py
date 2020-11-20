from pyspark.sql import SparkSession
import pandas as pd
import matplotlib
from pyspark.sql.functions import split, udf, desc, expr
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder.getOrCreate()
dfLog = spark.read.text("../data/NASA_access_log_Jul95.gz")
# dfLog.printSchema()

#
#dfLog.show(5, truncate=False)
#split column 'value' at spaces
df = dfLog.withColumn("tokenized", split("value", " "))
# df.show(5, truncate=False)


''' host          = match.group(1)
    client_id     = match.group(2)
    user_id       = match.group(3)
    date_time     = match.group(4)
    method        = match.group(5)
    endpoint      = match.group(6)
    protocol      = match.group(7)
    response_code = int(match.group(8))
    content_size  = match.group(9)
'''

@udf(MapType(StringType(),StringType()))
def parseUDF(line):
    import re
    PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S+)\s*" (\d{3}) (\S+)'

    match = re.search(PATTERN, line)
    if match is None:
        return (line, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = 0
    else:
        size = match.group(9)
    return {
        "host": match.group(1),
        "client_identd": match.group(2),
        "user_id": match.group(3),
        "date_time": match.group(4),
        "method": match.group(5),
        "endpoint": match.group(6),
        "protocol": match.group(7),
        "response_code": int(match.group(8)),
        "content_size": size
    }

dfParsed = dfLog.withColumn("parsed", parseUDF("value"))
# dfParsed.limit(5).toPandas()
# dfParsed.show(5, truncate=False)
# dfParsed.printSchema()
# dfParsed.selectExpr("parsed['host'] as host").show(5)

dfParsed.selectExpr(["parsed['host']", "parsed['date_time']"]).show(5)
fields = ['host', 'client_identd', 'user_id', 'date_time', 'method', 'endpoint', 'protocol', 'response_code', 'content_size']
exprs = ["parsed['{}'] as {}".format(field, field) for field in fields]
print(exprs)

#now each key is a column
dfClean = dfParsed.selectExpr(*exprs)
# dfClean.show(5)

#find the most popular endpoints
# dfClean.groupBy("host").count().orderBy(desc("count")).show(5)


#large files

# dfClean.createOrReplaceTempView("clean_typed_log")
# spark.sql("""
# select endpoint, content_size
# from clean_typed_log
# order by content_size desc
# """).show(5)

#this returns content_size but not in order because it's string typed
#so we need it in int type

dfCleanTyped = dfClean.withColumn("content_size_bytes", expr("cast(content_size as int)"))
# dfCleanTyped.show(5)
dfCleanTyped.createOrReplaceTempView("clean_typed_log")
spark.sql("""
select endpoint, content_size_bytes
from clean_typed_log
order by content_size_bytes desc
""").show(5)