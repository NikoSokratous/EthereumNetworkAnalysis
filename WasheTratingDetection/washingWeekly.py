from mrjob.job import MRJob
from mrjob.step import MRStep
import time as time

class top10miners(MRJob):
	def mapper(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==7:
				add1=fields[1]
				add2=fields[2]
				val=float(fields[3])
				timestamp = int(fields[6])
				week = time.strftime("%W", time.gmtime(timestamp))
				#week starting from monday
				year = time.strftime("%y", time.gmtime(timestamp))
				timelimit=(year,week)
				if val>0:
					yield ((timelimit,val), (add1,add2,val))
		except:
			pass
	def reducer(self, key, elements):
		add1=[]
		add2=[]		
		val1=[]
		for i in elements:
			add1.append(i[0])
			val1.append(i[2])
			add2.append(i[1])
		for i in range(len(add1)):
			if add1[i] in add2:
				yield(add1[i],val1[i])

if __name__ == '__main__':
	top10miners.run()
