from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("findmintemp")
sc = SparkContext(conf =conf)

def getMaxValue(Object):
    line = Object.split(',')
    StationId = line[0]
    Max_Min = line[2]
    temp = line[3]
    return (StationId,Max_Min,temp)
    
getValue = sc.textFile("file:///SparkCourse/1800.csv")
getMaxValue = getValue.filter(lambda x : "TMAX" in x[2])
stationMax = getMaxValue.map(lambda x : (x[0],x[2]))

maxtemp = stationMax.reduceByKey(lambda x,y:max(x,y))
results = maxtemp.collect();

for result in results:
    print (result[0] + "\t{:.2f}F".format(result[1]))
       
    
    