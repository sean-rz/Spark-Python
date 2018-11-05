import re
from pyspark import SparkConf, SparkContext


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster('local').setAppName('WordCountRegex')
sc = SparkContext(conf = conf)

input_rdd = sc.textFile('file:///Spark-Python/basics/book.txt')
words_rdd = input_rdd.flatMap(normalize_words)
word_counts = words_rdd.countByValue()

for word, count in word_counts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))

