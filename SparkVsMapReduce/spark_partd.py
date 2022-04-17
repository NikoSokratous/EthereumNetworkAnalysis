import re
import pyspark

sc = pyspark.SparkContext()

def is_good_line_t(line):
    try:
        fields = line.split(",")
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        return False

def is_good_line_c(line):
    try:
        fields = line.split(",")
        if len(fields) != 5:
            return False
        float(fields[3])
        return True
    except:
        return False

lines_c = sc.textFile('/data/ethereum/contracts')
filter_c = lines_c.filter(is_good_line_c)
address_c = filter_c.map(lambda l:(l.split(",")[0],1))

lines_t = sc.textFile('/data/ethereum/transactions')
filter_t = lines_t.filter(is_good_line_t)
address_t = filter_t.map(lambda l: (l.split(",")[2],float(l.split(",")[3])))
results = address_t.join(address_c)
reduced = results.reduceByKey(lambda (a,b), (c,d): (float(a) + float(c), b+d))

top10 = reduced.takeOrdered(10, key = lambda x: -x[1][0])

for i in top10:
    print(i)
