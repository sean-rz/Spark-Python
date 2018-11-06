from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("file:///Spark-Python/advanced/marvel-names.txt")
namesRdd = names.map(parseNames)  # key/value RDD

lines = sc.textFile("file:///Spark-Python/advanced/marvel-graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)

# key is the count and value is the superhero id
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0])) 

'''
Collect will bring the whole RDD to the driver code, max will be able to run on 
all partitions and bring back the max, so max is part of RDD API and the end results 
we need is just the max, so no need to bring the whole ma to local in the context 
of this example.
'''
mostPopular = flipped.max() # find the max key value

'''
lookup() does returns a *list* of all *values* associated with a given *key*. So, 
namesRdd.lookup(mostPopular[1]) returns a list of all of the superhero names 
(the values from the mapper) that match the superhero ID (the key) in mostPopular[1]. 
The [0] then just takes the first entry in that list. In this case, there is only one 
entry because we have a 1:1 mapping of names to ID's, but we still must extract that 
element from the list. Otherwise, when we try to print the results, we would get an 
error about trying to concatenate a list value instead of a string.
'''
# lookup target is an RDD as well
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
