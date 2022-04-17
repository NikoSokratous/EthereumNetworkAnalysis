from mrjob.job import MRJob
import re
import time as time
import statistics
from datetime import datetime

class fork(MRJob):

	def mapper(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==7:
				timestamp = int(fields[6])
				d=time.gmtime(float(fields[6]))
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%Y", time.gmtime(timestamp))
				day = time.strftime("%d", time.gmtime(timestamp))
				gas = float(fields[5])
				if d.tm_year==2017 and d.tm_mon==10:
					yield (day, gas)
		except:
			pass
	def combiner(self, date, value):
		count=0
		total=0
		for i in value:
			count+=1
			total+=i
		yield(date,(count,total))

	def reducer(self, date, value):
		count=0
		total=0
		for i in value:
			count+=i[0]
			total+=i[1]
		average=total/count
		yield (date, average)



if __name__ == '__main__':
	fork.run()
