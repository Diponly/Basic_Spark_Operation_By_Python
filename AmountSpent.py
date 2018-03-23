from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountSpentByCustomer")
sc = SparkContext(conf=conf)

def parseline(line):
    field = line.split(',')
    customer = field[0]
    amount = field[2]
    return(int(customer),float(amount))
    
input = sc.textFile("file:///SparkCourse/customer-orders.csv")
Customer_Amount = input.map(parseline)

##Total Amount spent

TotalAmountSpent = Customer_Amount.reduceByKey(lambda x,y:x+y)
results = TotalAmountSpent.collect();

for result in results:
    print (result)
