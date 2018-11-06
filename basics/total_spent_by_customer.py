"""
add up the total amount spent for each unique customer ID, 
"""

def extract_fileds(field):
    extracted = field.split(',')
    return (int(extracted[0]),float(extracted[2]))

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('SpendByCustomer')
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Spark-Python/basics/customer-orders.csv")
fields = lines.map(extract_fileds) # key/value rdd
total_amounts = fields.reduceByKey(lambda x,y: x+y)

results = total_amounts.collect()

for result in results:
    print(str(result[0]) + ", " + str(result[1]))

'''
def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))
     
input = sc.textFile("file:///sparkcourse/customer-orders.csv")
     
mappedInput = input.map(extractCustomerPricePairs)
     
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
     
results = totalByCustomer.collect()
'''