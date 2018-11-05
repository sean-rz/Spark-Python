"""
reduceByKey(): combine values with the same key using some function.  rdd.reduceByKey(lambda x, y: x + y) adds them up.
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")  # one process/thread
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)  # age is the key and numFriends is the value


lines = sc.textFile("file:///Spark-Python/basics/fakefriends.csv")
rdd = lines.map(parseLine)  # create the key/value RDD


totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
'''
mapValues:
(33, 385) => (33, (385, 1))
(33, 2) => (33, (2, 1))
reduceByKey:
(33, (387, 2))
'''

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# (33, (387, 2)) => (33, 193.5)

results = averagesByAge.collect()
for result in results:
    print(result)
