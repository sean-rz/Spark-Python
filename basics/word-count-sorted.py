import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Spark-Python/basics/book.txt")
words = input.flatMap(normalizeWords)


'''
Let's use RDD's instread of countByValue to keep it scalable
convert each word to a key/value pair with a value of 1
Then count them all up with reduceByKey()
'''

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Flip the (word, count) pairs to (count, word)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
