from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import time as time
import statistics
from datetime import datetime

class gas_top10(MRJob):

	top10=["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444","0xfa52274dd61e1643d2205169732f29114bc240b3","0x7727e5113d1d161373623e5f49fd568b4f543a9e","0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef","0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8"]
	def mapper1(self, _, line):
		fields = line.split(",")
		try:
			if len(fields)==7 and fields[2] in self.top10:
				timestamp = int(fields[6])
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%y", time.gmtime(timestamp))
				yearmonth=(year,month)
				gas_price = float(fields[5])
				blocks=int(fields[0])
				address=fields[2]
				yield (blocks,(yearmonth,address,gas_price,1))
			if len(fields)==9:
				blocks=int(fields[0])
				diff=float(fields[3])
				gas_used=int(fields[6])
				yield (blocks,(diff,gas_used,2,2))
		except:
			pass

	def reducer1(self, key, value):
		p=False
		gas_price=0
		diff=0
		gas_used=0
		for val in value:
			if val[3]==1:
				date=val[0]
				add=val[1]
				gas_price+=val[2]
				p=True
			else:
				diff+=val[0]
				gas_used+=val[1]
		if p:
			yield((date,add),(gas_price,gas_used,diff))

	def mapper2(self, key, value):
		yield(key,value)
	def combiner1(self, key, value):
		count=0
		tdiff=0
		tgas_price=0
		tgas_used=0
		for i in value:
			count+=1
			tgas_price+=i[0]
			tgas_used+=i[1]
			tdiff+=i[2]
		yield(key,(count,tgas_price,tgas_used,tdiff))

	def reducer2(self, key, value):
		count=0
		tdiff=0
		tgas_price=0
		tgas_used=0
		for i in value:
			count+=i[0]
			tgas_price+=i[1]
			tgas_used+=i[2]
			tdiff+=i[3]
		avgdiff=tdiff/count
		avg_gas_price=tgas_price/count
		avg_gas_used=tgas_used/count
		yield (key, (avgdiff,avg_gas_price,avg_gas_used))



	def steps(self):
		return[MRStep(mapper=self.mapper1 ,reducer=self.reducer1),MRStep(mapper=self.mapper2, combiner=self.combiner1, reducer=self.reducer2)]

if __name__ == '__main__':
	gas_top10.run()
