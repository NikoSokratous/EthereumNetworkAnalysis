from mrjob.job import MRJob
import re
import time as time
import statistics
from datetime import datetime

class AverageValue(MRJob):
# this class will define two additional methods: the mapper method goes here
	def mapper(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==7:
				timestamp = int(fields[6])
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%y", time.gmtime(timestamp))
				yearmonth=(year,month)
				Value = float(fields[3])
				yield (yearmonth, Value)
		except:
			pass
	def combiner(self, yearmonth, value):
		count=0
		total=0
		for i in value:
			count+=1
			total+=i
		yield(yearmonth,(count,total))

	def reducer(self, month, value):
		count=0
		total=0
		for i in value:
			count+=i[0]
			total+=i[1]
		average=total/count
		yield (month, average)



#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
	AverageValue.run()
