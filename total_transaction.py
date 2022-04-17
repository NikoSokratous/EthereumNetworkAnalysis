from mrjob.job import MRJob
import re
import time as time
from datetime import datetime

class total_t(MRJob):
	def mapper(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==7:
				timestamp = int(fields[6])
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%y", time.gmtime(timestamp))
				yearmonth=(year,month)
				yield (yearmonth, 1)
		except:
			pass
	def reducer(self, month, transactions):
		total=sum(transactions)
		yield (month, total)
	def combiner(self, month, transactions):
		total=sum(transactions)
		yield (month, total)



#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
	total_t.run()
