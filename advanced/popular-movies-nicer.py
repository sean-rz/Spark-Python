"""
Instead of keeping a table loaded in the driver program, we broadcast (transfer)
objects once to the nodes/executers, such that they are always there whenever needed
"""
from pyspark import SparkConf, SparkContext

# return a dictionary to map movie ids to movie names
def loadMovieNames():
    movieNames = {}
    with open("../basics/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())  # broadcat the dict variable to all nodes

lines = sc.textFile("file:///Spark-Python/basics/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

# get the value from dictionary
sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0])) # countMovie[1] = mivie id

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
