# find the most watched movies
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Spark-Python/basics/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))  # extract movie ids
movieCounts = movies.reduceByKey(lambda x, y: x + y) # how many times each movie appears

flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
