from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster('local').setAppName("get_popular_superhero")
sc = SparkContext(conf=conf)

## Get the names

def get_marvel_name(object):
    input = object.split('\"')
    id = input[0]
    name = input[1]
    return (int(id),name.encode('utf8'))
    
## get the marvel_remark

def countConcorrances(line):
    elements = line.split()
    return (int(elements[0]),len(elements)-1)

## get the data

names = sc.textFile("file:///SparkCourse/Marvel-names.txt")
namesRDD = names.map(get_marvel_name)

## get the graph details

getdata = sc.textFile("file:///SparkCourse/Marvel-graph.txt")
lines = getdata.map(countConcorrances)
getTotal = lines.reduceByKey(lambda x,y : x+y)
FlippedTotal = getTotal.map(lambda x : (x[1],x[0]))
mostpopular = FlippedTotal.min()

## Look Up to get the Movie Name

MostPopularName = namesRDD.lookup(mostpopular[1])[0]

print(MostPopularName ,mostpopular[0])

    
