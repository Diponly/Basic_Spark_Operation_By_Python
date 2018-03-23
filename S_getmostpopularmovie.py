# movielens dataset =>  user ID, a movie ID, a rating, and a timestamp
# Examining most popular movie i.e the movie watched more 

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("GetMostPopularMovie")
sc = SparkContext(conf = conf)

def getData(Object):
    return  (int(Object.split()[1]))
    
    
## Get the data

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")
Movie = data.map(getData)
tagMovie = Movie.map(lambda x : (x,1))
popularMovie = tagMovie.reduceByKey(lambda x,y : (x+y))
flipped = popularMovie.map(lambda x : (x[1],x[0]))
Sort = flipped.sortByKey()
result = Sort.collect();

for res in result:
    print (res)
    

