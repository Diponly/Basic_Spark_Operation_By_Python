import sys
from pyspark import SparkConf, SparkContext
from math import sqrt




def loadMovienames():
    movieNames = {}
    with open("file:///SparkCourse/ml-100k/u.ITEM") as f:
        for line in f:
            field = line.split('|')
            movieNames[int(field(0))] = field[1].decode('ascii', 'ignore')
        return movieNames
        


print('\nLoading Movie..')

## Keep it a key-value pair 

def filterDuplicates (userid,ratings):
    (movie1,rating1) = ratings[0]
    (movie2,rating2) = ratings[1]
    return movie1 < movie2
## make pairs

def makePairs(user,ratings):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))
    
  
conf = SparkConf().setMaster("local[*]").setAppName('RecommendMovieSimilarity')
sc = SparkContext(conf=conf)            

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")

##map rating to key/value pair  : (userid => movieid,rating)

rating = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

## self join to get all the rating given by the user

joinedRating = rating.join(rating)

## Unique joins

UniquejoinedRating = joinedRating.filter(filterDuplicates)

## Make pairs

moviePairs = UniquejoinedRating.map(makePairs)

## movie pair ratings

moviepairRatings = moviePairs.groupByKey()

result = moviepairRatings.collect();

for results in result:
    print (result)


        