from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('SpendByCustomer')
sc = SparkContext(conf = conf)

def extract_fileds(field):
    extracted = field.split(',')
    return (int(extracted[0]),float(extracted[2]))

lines = sc.textFile("file:///Spark-Python/basics/customer-orders.csv")
fields = lines.map(extract_fileds) # key/value rdd
total_amounts = fields.reduceByKey(lambda x,y: x+y)

total_amounts_flip = total_amounts.map(lambda x:(x[1],x[0])).sortByKey()

results = total_amounts_flip.collect()

for result in results:
    print(str(result[1]) + ", " + str(result[0]))