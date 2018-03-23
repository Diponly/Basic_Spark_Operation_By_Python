# movielens dataset =>  user ID, a movie ID, a rating, and a timestamp
# Examining most popular movie i.e the movie watched more 
# Braodcast is a sparkcontext object which allow us to do look up across cluster without memory compromise



from pyspark import SparkConf,SparkContext

## Get the data for broadcast

def getmoviename():
    #define empty dictionary
    MovieNames = {}
    with open ("ml-100k/u.ITEM") as f:
        for line in f:
            field = line.split('|')
            MovieNames[int(field[0])] = field[1]
    return MovieNames
    
    

conf = SparkConf().setMaster("local").setAppName("GetMostPopularMovie")
sc = SparkContext(conf = conf)

def getData(Object):
    return  (int(Object.split()[1]))
    
namedict = sc.broadcast(getmoviename())

## Get the data

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")
Movie = data.map(getData)
tagMovie = Movie.map(lambda x : (x,1))
popularMovie = tagMovie.reduceByKey(lambda x,y : (x+y))
flipped = popularMovie.map(lambda x : (x[1],x[0]))
Sort = flipped.sortByKey()
Sortedmoviewithnames = Sort.map(lambda x : (namedict.value[x[1]],x[0]))
result = Sortedmoviewithnames.collect();

for res in result:
    print (res)
    

