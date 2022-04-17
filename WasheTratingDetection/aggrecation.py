from mrjob.job import MRJob
import time as time
from mrjob.step import MRStep

class agg(MRJob):
	def mapper1(self, _, line):
		fields = line.split("\t")
		try:
			if len(fields)==2:
				address=fields[0]
				value=float(fields[1])
				yield (address,value)
		except:
			pass
	def combiner1(self, key, value):
		yield(key,sum(value))
	def reducer1(self, key, value):
		yield(key,sum(value))

	def mapper2(self, key, value):
		yield(None, (key,value))
	def reducer2(self, key, value):
		top = sorted(value, reverse=True, key=lambda x: x[1])
		rank=1
		for contract in top:
			yield(rank,contract)
			rank +=1
	def steps(self):
		return[MRStep(mapper=self.mapper1 ,combiner=self.combiner1 ,reducer=self.reducer1),MRStep(mapper=self.mapper2, reducer=self.reducer2)]

if __name__ == '__main__':
	agg.run()
