from pyspark import SparkConf, SparkContext
import collections

# Set up our contect
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Spark-Python/basics/ml-100k/u.data") # load the data into lines RDD
ratings = lines.map(lambda x: x.split()[2]) # Extract the movie rating and put it into ratings RDD
result = ratings.countByValue() # creates tuples 3 -> (3,2)

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
