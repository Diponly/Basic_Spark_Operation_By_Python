## Avg friends by age
from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendCountByAge")
sc = SparkContext(conf = conf)

## Get the Age and Friends Count

def parseline(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return(age,numFriends)
    
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseline)
totalfriendsByAge = rdd.mapValues(lambda x :(x,1)).reduceByKey(lambda x,y: (x[0] +y[0],x[1]+y[1]))

## (33,385) => (33,(385,1)),(33,2) => (33,(2,1)) == mapper
## (33,(387,2))

avgValue = totalfriendsByAge.mapValues(lambda x:x[0]/x[1])

## (33,387/2)
results = avgValue.collect()

for result in results:
    print (result)