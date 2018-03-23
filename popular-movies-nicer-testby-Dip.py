from pyspark import SparkConf,SparkContext

def loadmovies():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            field = line.split("|")
            movieNames[int(field[0])] = field[1]
        return movieNames
        
conf = SparkConf().setMaster("local").setAppName("popularMovies")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadmovies())

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

movies = lines.map(lambda x:(int(x.split()[1]),1))
moviescount = movies.reduceByKey(lambda x,y :x+y)
flipped = moviescount.map(lambda x:(x[1],x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie:(nameDict.value[countMovie[1]],countMovie[0]))

results= sortedMoviesWithNames.collect()
for result in results:
    print (result)
    






            