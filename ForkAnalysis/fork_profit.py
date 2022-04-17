from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time as time
import statistics
from datetime import datetime

class forkp(MRJob):

	def mapper1(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==7:
				d=time.gmtime(float(fields[6]))
				add1=fields[1] #from
				add2=fields[2] #to
				val=float(fields[3])
				val2=0-val
				if d.tm_year==2017 and d.tm_mon==10:
					yield (add1, val2)
					yield (add2, val)
		except:
			pass

	def combiner1(self, key, value):
		yield(key,sum(value))
	def reducer1(self, key, value):
		yield(key,sum(value))

	def mapper2(self, key, value):
		yield(None, (key,value))
	def reducer2(self, key, value):
		top5 = sorted(value, reverse=True, key=lambda x: x[1])
		rank=1
		for element in top5[:5]:
			yield(rank,element)
			rank +=1
	def steps(self):
		return[MRStep(mapper=self.mapper1 ,combiner=self.combiner1 ,reducer=self.reducer1),MRStep(mapper=self.mapper2, reducer=self.reducer2)]


if __name__ == '__main__':
	forkp.run()
