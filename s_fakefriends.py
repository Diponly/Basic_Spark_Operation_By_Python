from pyspark import SparkSession
from pyspark.sql import Row
import collections

spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///c:/temp").appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]),name = str(fields[1].encode("utf-8")),age = int(fields[2]),numFriends = int(fields[3]))

lines = spark.SparkContext.textFile("file:///c:/SparkCourse/fakefriends.csv")
people = lines.map(mapper)

##InferSchema and register the dataFrame

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")


teenagers  = spark.sql("SELECT * FROM people age >= 13 AND age <=19")

for teen in teenagers.collect():
    print (teen)
    
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()