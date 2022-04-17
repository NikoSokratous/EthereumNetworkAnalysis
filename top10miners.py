from mrjob.job import MRJob
from mrjob.step import MRStep


class top10miners(MRJob):
# this class will define two additional methods: the mapper method goes here
	def mapper1(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==9:
				miner=fields[2]
				size=int(fields[4])
				yield (miner, size)
		except:
			pass
	def combiner1(self, key, blocks):
		yield (key,sum(blocks))
	def reducer1(self, key, blocks):
		try:
			yield(key,sum(blocks))
		except:
			pass
	def mapper2(self, key, blocks):
		yield(None, (key,blocks))
	def reducer2(self, key, blocks):
		top10 = sorted(blocks, reverse=True, key=lambda x: x[1])
		rank=1
		for contract in top10[:10]:
			yield(rank,contract)
			rank +=1
	def steps(self):
		return[MRStep(mapper=self.mapper1 ,combiner=self.combiner1 ,reducer=self.reducer1),MRStep(mapper=self.mapper2, reducer=self.reducer2)]



#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
	top10miners.run()
