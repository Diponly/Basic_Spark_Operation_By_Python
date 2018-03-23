from pyspark import SparkConf ,SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/book.txt")

rdd = lines.flatMap(lambda x:x.split())

## The above lines results into everyword split

wordsCount = rdd.countByValue()

for word,count in wordsCount.items():
    cleanword =word.encode('ascii','ignore')
    if(cleanword):
        print (cleanword,count)

