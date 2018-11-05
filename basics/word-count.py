from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Spark-Python/basics/book.txt") # one paragraph per line
words = input.flatMap(lambda x: x.split()) # take every individual line of text and split it up into words

wordCounts = words.countByValue()  # how many times each unique value occurs

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
