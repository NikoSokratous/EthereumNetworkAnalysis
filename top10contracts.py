from mrjob.job import MRJob
from mrjob.step import MRStep

class top10(MRJob):
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				join_key = fields[2]
				join_value = float(fields[3])
				yield (join_key, (1,join_value))
			elif len(fields) == 5:
				join_key2 = fields[0]
				yield (join_key2, (2,1))
		except:
			pass
	def reducer1(self, key, elements):
		iscontract = False
		values = []
		for element in elements:
			if element[0]==1:
				values.append(element[1])
			elif element[0] == 2:
				iscontract = True
		if iscontract:
			yield key, sum(values)

	def mapper2(self, key,value):
		yield None, (key,value)

	def reducer2(self, _, keys):
		top10 = sorted(keys, reverse = True, key = lambda x: x[1])
		rank=1
		for value in top10[:10]:
			yield (rank,(value[0], value[1]))
			rank+=1

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	top10.run()
