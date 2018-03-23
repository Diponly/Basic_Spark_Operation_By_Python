from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster('local').setAppName('Degeeofseparation_Superhero')
sc = SparkContext(conf=conf)

startingCharacterID = 5306
targetCharacterID = 14

hitCounter = sc.accumulator(0)

## convert to a problem like return (heroID, (connections, distance, color)) 

def convertTOBFS(line):
    field = line.split()
    heroID = int(field[0])
    connections = []
    for connection in field[1:]:
        connections.append(int(connection))
    color = "WHITE"
    distance = 9999
    
    if(heroID == startingCharacterID):
        color = "GRAY"
        distance = 0
        
    return(heroID,(connection,distance,color))
        
## Get the Data

def CreateStartingRDD():
    input = sc.textFile("file:///SparkCourse/marvel-graph.txt")
    return input.map(convertTOBFS)
    
def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]
    
    results = []
       
    ## if this nodes needs to be expanded
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            
            if (targetCharacterID == connection):
                hitCounter.add(1)
            
            newentry = (newCharacterID,([],newDistance,newColor))
            results.append(newentry)
            
        color = 'BLACK'
        results.append((characterID,(connection,distance,color)))
        return results

def bfsReduce(data1,data2):
    edges1 = data1[0]
    edges2 = data2[0]
    
    distance1 = data1[1]
    distance2 = data2[1]
    
    color1 = data1[2]
    color2 = data2[2]
    
    distance = 9999
    color = 'WHITE'
    edges = []
    
    if(len(edges1) >0):
        edges.extend(edges1)
    if(len(edges2)>0):
        edges.extend(edges2)
        
    ##preserve minimum distince
    
    if(distance < distance1):
        distance = distance1
    if(distance2 < distance):
        distance = distance2
    
   ## preserve the darkest color
   
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2
    
    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1
        
        
    return (edges,distance,color)
    
       
iterationRDD = CreateStartingRDD()

for iteration in range(0,10):
    print("Running BFS iteration #" + str(iteration+1))
    
    mapped = iterationRDD.flatMap(bfsMap)
    
    print(mapped.count())
    
    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) + " different direction(s).")
        break
    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
        
    
    

    
        
            
            

    
    

    
    