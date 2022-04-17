from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time as time
import statistics
from datetime import datetime

class gas_diff(MRJob):

	def mapper1(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==5:
				blocks=int(fields[3])
				yield (blocks,(1,1,1,1))
			if len(fields)==9:
				blocks=int(fields[0])
				diff=float(fields[3])
				gas_used=int(fields[6])
				timestamp = int(fields[7])
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%y", time.gmtime(timestamp))
				date=(year,month)
				yield (blocks,(diff,gas_used,date,2))
		except:
			pass

	def reducer1(self, key, value):
		p=False
		diff=[]
		gas_used=[]
		date=[]
		for val in value:
			if val[3]==1:
				p=True
			else:
				diff.append(val[0])
				gas_used.append(val[1])
				date.append(val[2])
		if p:
			if date and diff and gas_used:
				for d,g,dif in zip(date,gas_used,diff):
					yield(d,(g,dif))


	def mapper2(self, key, value):
		yield(key,value)

	def reducer2(self, key, value):
		count=0
		tdiff=0
		tgas_used=0
		for i in value:
			count+=1
			tgas_used+=i[0]
			tdiff+=i[1]
		avgdiff=tdiff/count
		avg_gas_used=tgas_used/count
		yield (key, (avgdiff,avg_gas_used))



	def steps(self):
		return[MRStep(mapper=self.mapper1 ,reducer=self.reducer1),MRStep(mapper=self.mapper2, reducer=self.reducer2)]

if __name__ == '__main__':
	gas_diff.run()
