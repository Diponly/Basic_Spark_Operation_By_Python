from pyspark import SparkConf ,SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("BetterByDip")
sc = SparkContext(conf=conf)

def normalizewords(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())
    

## Define the RDD
input =  sc.textFile("file:///SparkCourse/book.txt")

##Pass the rdd to get the flatmap

NicerWord = input.flatMap(normalizewords)
##wordcount = NicerWord.countByValue()

wordcount = NicerWord.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
sortedWord = wordcount.map(lambda x:(x[1],x[0])).sortByKey()

results = sortedWord.collect()
## Pass this through 

for value,word in results.items():
    cleanword = word.encode('ascii','ignore')
    if(cleanword):
        print (cleanword.decode()+' ' ,str(value))
    
    
    


