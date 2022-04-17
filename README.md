# Ethereum Network Analysis
This project is using data from Google BigQuery to analyse and make conclusion about ethereum price and network complexity

<h2>First job: Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.</h2>

In this part, we have been asked to use Hadoop MapReduce to count the number of transactions and aggregated them for each month in our data set. 
The first thing to do was to import the time library so that we can use it, to go from timestamps that our data have to months and years.  Then we made a mapper that yielded a key value, and a tuple of the date (year, month) and 1 as its value. Finally, a combiner and a reducer were created to sum all the values for each key (year, month).

Then excel was used to help with the visualization of the data in the form of a bar graph.

<img src="https://user-images.githubusercontent.com/97196020/163735461-6f3360c5-5b08-41ff-bf5c-9d9613e501b8.png" width="700" >

Now we can see that the number of transactions in the network generally increases, except for a spike during 2017. We expected to see that because the price of Ethereum was also increased during that time, we were in a “bull” cycle. A decrease was followed that indicated the “bear” market that follows with a local minimum at 02/2019. Then the transaction volume increases again indicating the “bull” run that will be followed. 

<h2>Second job: Create a bar plot showing the average value of transactions in each month between the start and end of the dataset.</h2>


In the second job, we have been asked to use Hadoop MapReduce to average the value of transactions for each month in our data set. 
The first thing to do was to import the time library so that we can use it, to go from timestamps that our data have to months and years.  Then we made a mapper that yielded a key value, and a tuple of the date (year, month) and the value of each transaction as its value. Finally, a combiner and a reducer were created to sum and average the value for each transaction of the keys (year, month).
Then excel was used to visualize the data in the form of a bar graph. 

<img src="https://user-images.githubusercontent.com/97196020/163735507-1f3b50a6-5f2e-4733-9095-01b36a627e6c.png" width="700" >

By looking at the graph we can first see that the value of each transaction drops as we move through time. An outlier to the trend is a spike during 2017 due to the bull market that happened that year. The general trend does not show the whole picture, those numbers are in Ethereum and Wei which is equal to e-18 ether. They are a volatile asset and does not have a stable value compared to other assets. We see that drop (mostly) because compared to a stable coin like usdc, wei rise in value. That means that even if the average value of transactions drops in Wei, in usdc stays relatively stable.  

<h2>TOP TEN MOST POPULAR SERVICES</h2>


In part B we have been asked to perform 3 different jobs. An Initial aggregation, then a job to join the transaction and contract documents and filter the results, and finally a job to print the top 10 contracts in the network. 
All jobs were performed using one single code. To achieve that we first had to import MRStep. Then in the first mapper we first split every line at a (‘,’), we can do that because both documents are CSV. To find what documents are in we check the length of it. If it has 7 fields, it is the transaction document and if it has 5 is the contracts.  If we are in the transaction folder, we want to yield the address and value of the transaction that address got and an identifier, if we are in the contracts document we just want to yield the address and another different identifier. Then we use a reducer to aggregate and filter our data. Inside the reducer, a Boolean variable was created with a default value of “false” and an empty table. The identifier was used to check from what file our data came. In the case that came from transactions, we append the value of the transaction in the table that we created before. In the case that came from contracts, we change the Boolean variable to True, which means that this address is indeed a contract. Then if the Boolean variable is true, we yield the address as a key value and the sum of the values in the table. Then we use a second mapper to put the address and value in a tuple. Finally, we use a second reducer to first sort the tuple and yield the address and the combined value with each address rank. 

1.	["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444", 8.415510080996624e+25]
2.	["0xfa52274dd61e1643d2205169732f29114bc240b3", 4.578748448318575e+25]
3.	["0x7727e5113d1d161373623e5f49fd568b4f543a9e", 4.5620624001352286e+25]
4.	["0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef", 4.317035609226235e+25]
5.	["0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8", 2.706892158202084e+25]
6.	["0xbfc39b6f805a9e40e77291aff27aee3c96915bdd", 2.1104195138094623e+25]
7.	["0xe94b04a0fed112f3664e45adb2b8915693dd5ff3", 1.5562398956804906e+25]
8.	["0xbb9bc244d798123fde783fcc1c72d3bb8c189413", 1.198360872920289e+25]
9.	["0xabbb6bebfa05aa13e908eaa492bd7a8343760477", 1.1706457177941126e+25]
10.	["0x341e790174e3a4d35b65fdc067b6b5634a61caea", 8.379000751917755e+24]

That didn’t give us the most popular services on the network though, but only the contracts of those services. To learn what those contracts represent Etherscan was used.

By searching the above address in Etherscan we can find who owns those addresses:
1.	Kraken(exchange)
2.	Kraken(exchange)
3.	Bitfinex(exchange)
4.	Poloniex(exchange)
5.	Gemini(exchange)
6.	Not none
7.	Bittrex(exchange)
8.	TheDAO(Decentralized Autonomous Organization)
9.	Replaysafesplit v2
10.	Kraken(exchange)

Finally, we can conclude that the most popular services in the network exchange as 7 out of 10 of the most popular contracts were owned by them. Then we have one contract that we could not find any information about, a DAO and the Replaysafesplit v2 contract referring to forks of “etc” and “eth”. 

<h2>PART C: TOP TEN MOST ACTIVE MINERS</h2>


In part C we have been asked to evaluate the top 10 miners in the network. In this case, we did not have to join or filter anything because all the information that we wanted was inside the blocks document. We had to aggregate and get the top 10 at the same code. That’s why we imported MRStep and created 2 mapper, 2 reducers, and one combiner. We use the first mapper to yield miner address, as a key and block size, as a value. Then we use the combiner and the first reducer, to sum up, all blocks mined by a miner. We use the second mapper to yield the miner address and the total blocks mined by the miner as a tuple. Finally, we sorter the tuple in the second reducer and print the top 10 values with their rank. 

1.	["0xea674fdde714fd979de3edf0f56aa9716b898ec8", 23989401188]
2.	["0x829bd824b016326a401d083b33d092293333a830", 15010222714]
3.	["0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c", 13978859941]
4.	["0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5", 10998145387]
5.	["0xb2930b35844a230f00e51431acae96fe543a0347", 7842595276]
6.	["0x2a65aca4d5fc5b5c859090a6c34d164135398226", 3628875680]
7.	["0x4bb96091ee9d802ed039c4d1a5f6216f90f81b01", 1221833144]
8.	["0xf3b9d2c81f2b24b0fa0acaaa865b7d9ced5fc2fb", 1152472379]
9.	["0x1e9939daaad6924ad004c2560e90804164900341", 1080301927]
10.	["0x61c808d82a3ac53231750dadc13c777b59310bd9", 692942577]


<h2>Popular Scams: Utilizing the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time, and does this correlate with certainly known scams going offline/inactive?</h2>

In part D we have been asked to find what are the most lucrative scams in the Ethereum network, how the value of those scams evolve through time, and how many of them are active right now.

<h3>First job (finding the most popular scams)</h3>


To do those, three different programs were developed. First to find the most popular scams we needed data from the transaction document and the scams document as well. To join those files, we importer MRStep. Then because the transaction file is a CSV and the scam file JSON type, we first check the length of the fields to find out in what file we are. In the case of the transaction file, we yield from the mapper the ‘to addresses’ (as a key) and the value of the transaction with an identifier (as a value). If we are in the scam file we yield the addresses, again, as a key and the category of the scam with an identifier as the value. 
Then the first reducer goes over every address and sums app all the values of the transactions. Then if that address has a category, that means that is in the scam JSON file, it yields the category with the sum of those values. Then we use another mapper, combiner, and reducer to sum those results. Now we have the category as the key.
The results are:

1. "Scamming"	3.833616286244429e+22
2. "Phishing"	2.699937579408743e+22
3. "Fake ICO"	1.35645756688963e+21

Those are the scams with the total amount of Wei each of them gained.

<h3>Second job (finding the status of the most popular scams, Counts, and Volume)</h3>

Counts: To find the status of those scams we only need to use the scams JSON file. At our mapper, we are yielding the category of each scam and the status as the key, and 1 as the value. Later we sum all our values together to see how many of those scams are active, offline, inactive, or suspended. 
The results are:

1. ["Fake ICO", "Offline"]	5
2. ["Phishing", "Inactive"]	3
3. ["Phishing", "Offline"]	714
4. ["Phishing", "Suspended"]	1
5. ["Scam", "Offline"]	1
6. ["Scamming", "Active"]	326
7. ["Phishing", "Active"]	157
8. ["Scamming", "Offline"]	2406
9. ["Scamming", "Suspended"]	1

Volume: Now, to find how the volume change regarding the status and the category of the scam we need to take data from both transactions and scams files. First, we check which file we are using the files of the file as a condition. For transaction files, we yield the address as a key and value and an identifier as the values. For the scams file, we yield again the address as the key and the category, the status, and an identifier as the values. Inside the reducer, first, we use the identifier to find out from what file our data came, and then if they came from the transaction file sum up all the values together. If they came from the scams file, we save the category and status inside two variables with the same names. Then we are yielding the category and the status as the keys and the sum of the values as the value. After that, we use one combiner and one reducer, to sum up, all the values for the category-status pair of keys.
The results are:

1. ["Fake ICO", "Offline"]	1.35645756688963e+21
2. ["Phishing", "Inactive"]	1.488677770799503e+19
3. ["Phishing", "Offline"]	2.245125123675149e+22
4. ["Phishing", "Suspended"]	1.63990813e+18
5. ["Scam", "Offline"]	0
6. ["Scamming", "Active"]	2.209695235667909e+22
7. ["Phishing", "Active"]	4.5315978714979385e+21
8. ["Scamming", "Offline"]	1.6235500337815093e+22
9. ["Scamming", "Suspended"]	3.71016795e+18

We can see there were a lot of scams about scamming and phishing both active now and offline. The most interesting observation though is that there was only 5 fake ICO and that makes the fake ICO scam the most lucrative of the three since the average value of each fake ICO is significantly larger than the other 2 categories. 

<h3>Third job (scams over time)</h3>

In the third job, we have been asked to perform a time analysis of the behavior of the scams. That means seeing how the value of each scam category change throughout time. First, to do that we need data from both the transaction file and the scams file. At our first mapper, we check in which file we are in, by checking the fields of each line, and then yield as our key, the address in both cases, and if we are in the transaction file, we also yield the value of each transaction, an identifier and the year and the month those transactions were made. If we are in the scams file we additionally yield our values, the category, and an identifier. 
Next, in our first reducer, we do the following for each address. If our address came for the scam folder, we save the category of it at a variable “p” and change a Boolean variable from false to true. If our value comes from the transaction folder we create two lists, one with the values of each transaction and one with the dates. Then we check if our address exists in the scams file with the help of “p” (if false does not exist, if true exists). If it exists, we loop throughout all the variables in the values and dates list and yield the date and the category, as our key now and the value as our value.
We use the second mapper and reducer, to sum up, all the values for each date and category pair. 

<img src="https://user-images.githubusercontent.com/97196020/163735763-81f21cee-03e0-4e93-ab97-120739f18949.png" width="700" >

We notice that fake ICO and phishing had their picks in the middle of 2017 during the bull market. Most interesting though is the fact that the pick for scamming was not during the bull market but during the crash, after the bull market, in 2018. 

<h2>Wash Trading: Which addresses are involved in wash trading? Which trader has the highest volume of wash trades? How certain are you of your result?</h2>

As we can see from the document washing is when money moves from one address to other addresses and finally back to the same one in a short period of time to fake the trading volume. The paper split washing trades by hourly, daily, or weekly. We will look and found accounts that engage in washing within our time limit, in the first attempt to be one hour and one day in the second case. Another key point that we can see in the paper is that before those funds come to the original address, they may pass through a cycle of trades first. That’s why in our code we are looking for trades made inside the time limit and send money from one address and another trade that has the same address as the receiver of the same amount of funds, skipping the in-between trades. 
To find those trades we made a MapReduce code that use the transaction file as an input. Our mapper first uses the timestamp of the trade to calculate the hour, day (1-365), year of the trade, the value, and the ‘from’ and ‘to address’. Then it yields the hour of the trade and the value as our key so that later we are checking at trades that happened in the same hour of the day and the value of the funds was the same, excluding trades with values equal to zero. We also yield the value of the trade, again, and the ‘from’ and ‘to address’. 
Later in the reducer, we create 3 tables, one for each of our values. Then we check if the ‘from address’ exists in the ‘to address table’. If it is, we are certain that inside the same hour same address sends and receives back the same amount of money. 
Then we create a different MapReduce code that takes the output of the first MapReduce and for each address sum up the value of the trades that that address made. 
We did the same some programs to identify daily and weekly washing with the only exception that our time limit now only contains day the (1-365) or the week and the year.

<b>Following are the top 20 addresses that are involve in washing trading, with hourly time limit:</b>

1.	["\"0x02459d2ea9a008342d8685dae79d213f14a87d43\"", 1.954853133249322e+25]
2.	["\"0x32362fbfff69b9d31f3aae04faa56f0edee94b1d\"", 5.2954982937342e+24]
3.	["\"0x0c5437b0b6906321cca17af681d59baf60afe7d6\"", 2.3771555719346709e+24]
4.	["\"0x8a288f63b9de32feeedd4c3fc3347f026b599dd1\"", 6.929999999999999e+23]
5.	["\"0x3c0cbb196e3847d40cb4d77d7dd3b386222998d9\"", 5.9182090000000004e+23]
6.	["\"0x04645af26b54bd85dc02ac65054e87362a72cb22\"", 4.787669999999997e+23]
7.	["\"0x60e16961ad6138d2fb3e556fc284d9c2fff41486\"", 4.6633200000000005e+23]
8.	["\"0x6f50c6bff08ec925232937b204b0ae23c488402a\"", 4.311810500000004e+23]
9.	["\"0xdb6fd484cfa46eeeb73c71edee823e4812f9e2e1\"", 4.164973682907082e+23]
10.	["\"0xe2cba51d6aaa99d1223a4934b4dcea10ee3496c3\"", 3.8869999999999644e+23]
11.	["\"0x2cc6d6f3701dfced7efefae71b51d281cc67f497\"", 3.8659999999999645e+23]
12.	["\"0x8efc337a1e5748f28d819dff77f14f65356345b2\"", 3.789999999999965e+23]
13.	["\"0xd14c70863b21a8902476d671db9e5fae53e96a12\"", 3.7699999999999655e+23]
14.	["\"0x51cea28458a187e1288bfa315303409f0f3dff88\"", 3.762999999999965e+23]
15.	["\"0x6ea853c1f4897b2a85ea1c16acd0eca6f88bc205\"", 3.7479999999999654e+23]
16.	["\"0x3563eaf59e3ffe2663aa7d6fc5d604549bfbf6f6\"", 3.720999999999966e+23]
17.	["\"0x2ba24c66cbff0bda0e3053ea07325479b3ed1393\"", 2.959101e+23]
18.	["\"0xd24400ae8bfebb18ca49be86258a3c749cf46853\"", 2.2705115957999997e+23]
19.	["\"0xb2a48f542dc56b89b24c04076cbe565b3dc58e7b\"", 2.0784500000000016e+23]
20.	["\"0x5174b9dc892e69d5e844ecb1dcee86c7d9359b7f\"", 1.8324925599999998e+23]

<b>Following are the top 20 addresses that are involve in washing trading, with daily time limit:</b>
1.	["\"0x02459d2ea9a008342d8685dae79d213f14a87d43\"", 1.954857162963381e+25]
2.	["\"0x32362fbfff69b9d31f3aae04faa56f0edee94b1d\"", 5.295498305534218e+24]
3.	["\"0x0c5437b0b6906321cca17af681d59baf60afe7d6\"", 2.3771629206326712e+24]
4.	["\"0x8a288f63b9de32feeedd4c3fc3347f026b599dd1\"", 2.0985e+24]
5.	["\"0x60e16961ad6138d2fb3e556fc284d9c2fff41486\"", 7.01644e+23]
6.	["\"0xba0fc403ae9379763e6f864cdc470139ebdd774e\"", 6.7172e+23]
7.	["\"0x04645af26b54bd85dc02ac65054e87362a72cb22\"", 6.04964999999998e+23]
8.	["\"0x3c0cbb196e3847d40cb4d77d7dd3b386222998d9\"", 6.018209e+23]
9.	["\"0x008024771614f4290696b63ba3dd3a1ceb34d4d9\"", 5.654652216635699e+23]
10.	["\"0x6f50c6bff08ec925232937b204b0ae23c488402a\"", 4.948063600000004e+23]
11.	["\"0xdb6fd484cfa46eeeb73c71edee823e4812f9e2e1\"", 4.174983682907082e+23]
12.	["\"0xe2cba51d6aaa99d1223a4934b4dcea10ee3496c3\"", 3.933999999999965e+23]
13.	["\"0x2cc6d6f3701dfced7efefae71b51d281cc67f497\"", 3.910999999999965e+23]
14.	["\"0x8efc337a1e5748f28d819dff77f14f65356345b2\"", 3.835999999999965e+23]
15.	["\"0x51cea28458a187e1288bfa315303409f0f3dff88\"", 3.812999999999966e+23]
16.	["\"0xd14c70863b21a8902476d671db9e5fae53e96a12\"", 3.803999999999966e+23]
17.	["\"0x6ea853c1f4897b2a85ea1c16acd0eca6f88bc205\"", 3.7799999999999664e+23]
18.	["\"0x3563eaf59e3ffe2663aa7d6fc5d604549bfbf6f6\"", 3.764999999999966e+23]
19.	["\"0xc257274276a4e539741ca11b590b9447b26a8051\"", 3.280996700000004e+23]
20.	["\"0xb96fbc8c9a25a62718a16f1cd323ab20c502fde4\"", 3.238691989619999e+23]

<b>Following are the top 20 addresses that are involve in washing trading, with daily time limit:</b>
1.	["\"0x02459d2ea9a008342d8685dae79d213f14a87d43\"", 1.954857162963382e+25]
2.	["\"0x32362fbfff69b9d31f3aae04faa56f0edee94b1d\"", 5.295510080733997e+24]
3.	["\"0x8a288f63b9de32feeedd4c3fc3347f026b599dd1\"", 3.4605010030000014e+24]
4.	["\"0x0c5437b0b6906321cca17af681d59baf60afe7d6\"", 2.37718329622987e+24]
5.	["\"0xb794f5ea0ba39494ce839613fffba74279579268\"", 1.775e+24]
6.	["\"0x008024771614f4290696b63ba3dd3a1ceb34d4d9\"", 1.739858219269311e+24]
7.	["\"0xc257274276a4e539741ca11b590b9447b26a8051\"", 1.0261989709999974e+24]
8.	["\"0x32be343b94f860124dc4fee278fdcbd38c102d88\"", 8.349059238008964e+23]
9.	["\"0x60e16961ad6138d2fb3e556fc284d9c2fff41486\"", 7.416439999999999e+23]
10.	["\"0xba0fc403ae9379763e6f864cdc470139ebdd774e\"", 6.8041e+23]
11.	["\"0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be\"", 6.497970255692013e+23]
12.	["\"0xb96fbc8c9a25a62718a16f1cd323ab20c502fde4\"", 6.395606389620004e+23]
13.	["\"0x04645af26b54bd85dc02ac65054e87362a72cb22\"", 6.240009899999986e+23]
14.	["\"0x3c0cbb196e3847d40cb4d77d7dd3b386222998d9\"", 6.118209e+23]
15.	["\"0xb21385af6bfd19d0e787d718fb83559e515412eb\"", 5.682439999999997e+23]
16.	["\"0x6cc5f688a315f3dc28a7781717a9a798a59fda7b\"", 5.634304237642044e+23]
17.	["\"0x876eabf441b2ee5b5b0554fd502a8e0600950cfa\"", 5.275103593656767e+23]
18.	["\"0x911048b3da1687f1dd3dbf1a1bdae1243d6b7f92\"", 5.2119520300000016e+23]
19.	["\"0x6f50c6bff08ec925232937b204b0ae23c488402a\"", 5.096097673950003e+23]

We can say with certainty that those 20 addresses are involved in wash trading. We can not say the same though for the bottom part of the list our program made, since there are addresses that have transferred a total value less than an ether, something that will not make any impact on the trading volume. 
Another limitation that our program has is that can only detect wash trades inside a window of an hour. If our trade starts exactly at the beginning of each hour. For example, if a trade happens at 3:30 and the return trade comes back at 4:30 our program will not detect it. Finally, our program only finds wash trades that happen exactly with the same values of funds. For example, if a wash trader sends a trade and receives back only 99.9% of that trade our program will miss it. 

<h2>Fork the Chain: There have been several forks of Ethereum in the past. Identify one or more of these and see what effect it had on price and general usage. For example, did a price surge/plummet occur, and who profited most from this?</h2>



<b>The Byzantium fork (Oct-16-2017 05:22:11 AM +UTC) was used for the fork the chain analysis. The 4 thing that the chain added to the network was:</b>

•	Reduced block mining rewards from 5 to 3 ETH.
•	Delayed the difficulty bomb by a year.
•	Added ability to make non-state-changing calls to other contracts.
•	Added certain cryptography methods to allow for layer 2 scaling.

To find out how the fork affects the price of the network, first, we take from the transaction file the time (year, month, day) and the gas price. Since the fork happen in the middle of the month, we decided to check the price through the whole month of October. If our date happens to be inside October 2017, we yield the date as the key and the gas price as the value. Then, we use a combiner and a reducer to find out the average gas price for each day and we use excel to graph the result. 

<img src="https://user-images.githubusercontent.com/97196020/163735837-a0d52f7c-bf04-4f04-97f6-6a4668aeac75.png" width="700" >

We notice a huge drop, 40.98% to be exact, in the gas price during the fork that stayed like that for the whole duration of the month after the fork. The reason that mainly happened is because the mining reward decreased from 5 to 3, a 40% decrease. That is almost the same percentage that the gas price drops. 

<b>Who profited the most:</b>

To find out who profited most during this fork we used once again the transaction file. First, we are finding the time that the transaction happened, and we add an if statement to check that we are inside October 2017 when the fork happens. It is best in my judgment to check all the month because someone could be profited from the fork hype (before the fork) and someone else could profit from the actual benefits that the fork gave (after the fork). If the transaction is inside October, we yield twice. First time by using the “from_address” as a key and the value of the transaction, as a negative value to indicate “loss”. The second time we are using the “to_address” as a key and the value of the transaction as a value, positive to indicate revenue. Then we are using a combiner and a reducer to sum up all values for each address. 

After that, we are using a second mapper to pass our values into a tuple and later sort the top 5 addresses in the second reducer.

The top 5 addresses that profited from the fork:
1.	["0x7727e5113d1d161373623e5f49fd568b4f543a9e", 2.7186253975798244e+24]
2.	["0xe94b04a0fed112f3664e45adb2b8915693dd5ff3", 8.837767969368459e+23]
3.	["0xfa52274dd61e1643d2205169732f29114bc240b3", 6.707564151793931e+23]
4.	["0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8", 6.243653193187001e+23]
5.	["0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef", 5.511487779507492e+23]

With the help of Etherscan we see that those addresses belong to:

1.	Bitfinex
2.	Bittrex 
3.	Kraken
4.	Gemini
5.	Poloniex

<h2>Gas Guzzlers: For any transaction on Ethereum a user must supply gas. How have gas prices changed over time? Have contracts become more complicated, requiring more gas, or less so? How does this correlate with your results seen within Part B.</h2>

To perform the above analysis, we created three different MapReduce programs, one to find how gas prices change throughout time, one to find how gas changes throughout time aka the complexity of each transaction, and one to see how that affected the top 10 contracts. 

<b>How gas prices change throughout time</b>

A simple MapReduce program was used with one mapper, one combiner, and one reducer. Then we used the timestamp of each trade to calculate the month and the year, and yield it as our key, yielding the gas price as the value. Then both in the combiner and the reducer, we average the gas price for each month of the year. 

Later we used those data in excel and created the following graph.

<img src="https://user-images.githubusercontent.com/97196020/163735910-6784f110-9874-4cfc-9e02-27620c66d97c.png" width="700" >


As we can see from the graph the gas price is reduced significantly throughout time with some periodical spikes every 2 years. That makes perfect sense even though the complexity and difficulty increase, as we will see later. Gas fees are credited to the miner who mined the transaction. Since the price of ether increases in the same period, even if the difficulty increase, the miner has the same motivation (same money-reward) in USD to keep mine. In contrast, if the amount gas fee was at the same levels that were in 2015, none will use the network anymore because this amount translates to a very high transaction fee in USD. 

<b>How the contract gas (complexity) change throughout time</b>

To find out how the complexity and difficulty change throughout time, we used both the contract and blocks files. We are yielding the block number from both files as the key and an identifier as the value. In addition to that, we are yielding inside a tuple, in the blocks file case, the difficulty of the gas used and the date (year, month) of the block. For each block number in the reducer, we first used a Boolean variable to check that the block is related to the creation of a contract and if it is we yield the date of the block as the key and the difficulty with the gas used as the values. We are using an additional mapper and reducer to find the average difficulty and gas used for every month. Then excel was used to present the data in the form of a graph. 

<img src="https://user-images.githubusercontent.com/97196020/163735955-184a7f3a-4ee0-426e-9c75-0dbc5947e9ef.png" width="700" >
<img src="https://user-images.githubusercontent.com/97196020/163735966-57f96cfb-79da-409a-8f01-3fd2304d6a17.png" width="700" >

We see that the complexity is stately increasing. As the popularity of the Ethereum network rises, more people are getting involved in mining, and more hashing power is added to the network. Thus, the difficulty must increase to ensure that blocks are not generated too quickly. That also has the impact that the reward for each block mined, drops but as we saw above because the price of eth rises, miners have the same motivation to keep mining as before.

<b>How does gas affect the top 10 contacts on the chain?</b>

The code to study how that affects the top 5 contracts used transaction and blocks. Before we go in our first mapper, we make a list in which in have inside the top 5 addresses that we got from the partb problem. Then we check if the address that we trying to give in the mapper is inside that list and only if there is, do we yield a key of the block number, and a tuple of values that are included inside the date (year, month), the address, the gas price, and an identifier. From the blocks file, we yield the block number as the key, the difficulty, the gas, used and an identifier as the value. Then in our first reducer for every block we go through all the values, using the identifier to check from which file our values came and if they came from the transaction, we save the date and the address inside variables, we sum up the gas price and turn Boolean variable from false to true to indicate that this contract belongs to the top 10 list. If our values came from the block contract, we sum up the difficulty and the gas used. Then with the help of the Boolean variable, we check and if that contract came from the top 10 list, we are yielding the date and the address as a join key and the gas price, gas used, and difficulty as the values. Then we use a second mapper a combiner and a second reducer to find the average values for every month and every address in our list.

1.	Kraken(exchange)
2.	Kraken(exchange)
3.	Bitfinex(exchange)
4.	Poloniex(exchange)
5.	Gemini(exchange)


<img src="https://user-images.githubusercontent.com/97196020/163735981-ee907759-c1cd-4593-a6a5-be9f2285bb58.png" width="700" >
<img src="https://user-images.githubusercontent.com/97196020/163735985-f2fa16b8-86bb-4fc0-9bac-cbbb7765125a.png" width="700" >
<img src="https://user-images.githubusercontent.com/97196020/163735988-fd73c8e4-a461-4119-b60c-49a31d22a468.png" width="700" >


In general difficulty and gas used to follow the same trends as the general difficulty and gas used graphs. Now, by looking at the gas price chart we notice that the first address’s gas price is a lot higher than any other address. The trend seems the same as the other address by the value of the gas price is bigger. That indicates a premium and means that the first address may be involved in high-speed trading activities. We came to that conclusion because in the Ethereum network a user can adjust the gas price and increase to have faster transaction times.

<h2>Comparative Evaluation Reimplement Part B in Spark (if your original was MRJob or vice versa). How does it run in comparison?</h2>

<b>MapReduce links:</b>

Job1:
http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22449/
http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22455/

Job2:
http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22461/
http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22474/

Job3:
http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22612/
http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1637317090236_22656/

Spark ID:

Link: http://itl106.student.eecs.qmul.ac.uk:4040

Job1 id: application_1648683650522_1703

Job2 id: application_1648683650522_1705

Job3 id: application_1648683650522_1706

We now must make the same program as partb but in spark. The logic that was used was the same as partb with only changing the syntax so that it can be run with spark. Following are the results from the spark program as well as the times that it took for the MapReduce and Spark programs to run. 

<b>Spark results</b>

1. (u'0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444', (8.415510080996211e+25, 322149))
2. (u'0xfa52274dd61e1643d2205169732f29114bc240b3', (4.578748448318603e+25, 1491246))
3. (u'0x7727e5113d1d161373623e5f49fd568b4f543a9e', (4.562062400135007e+25, 513002))
4. (u'0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef', (4.3170356092264115e+25, 1811324))
5. (u'0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8', (2.7068921582017816e+25, 289823))
6. (u'0xbfc39b6f805a9e40e77291aff27aee3c96915bdd', (2.110419513809482e+25, 362046))
7. (u'0xe94b04a0fed112f3664e45adb2b8915693dd5ff3', (1.5562398956803675e+25, 1616593))
8. (u'0xbb9bc244d798123fde783fcc1c72d3bb8c189413', (1.1983608729203556e+25, 173561))
9. (u'0xabbb6bebfa05aa13e908eaa492bd7a8343760477', (1.1706457177941104e+25, 710462))
10. (u'0x341e790174e3a4d35b65fdc067b6b5634a61caea', (8.379000751917756e+24, 42))

<b>MapReduce results</b>

1.	["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444", 8.415510080996624e+25]
2.	["0xfa52274dd61e1643d2205169732f29114bc240b3", 4.578748448318575e+25]
3.	["0x7727e5113d1d161373623e5f49fd568b4f543a9e", 4.5620624001352286e+25]
4.	["0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef", 4.317035609226235e+25]
5.	["0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8", 2.706892158202084e+25]
6.	["0xbfc39b6f805a9e40e77291aff27aee3c96915bdd", 2.1104195138094623e+25]
7.	["0xe94b04a0fed112f3664e45adb2b8915693dd5ff3", 1.5562398956804906e+25]
8.	["0xbb9bc244d798123fde783fcc1c72d3bb8c189413", 1.198360872920289e+25]
9.	["0xabbb6bebfa05aa13e908eaa492bd7a8343760477", 1.1706457177941126e+25]
10.	["0x341e790174e3a4d35b65fdc067b6b5634a61caea", 8.379000751917755e+24]

<b>Spark time:</b> to find the time it took a spark to run we run the program 3 times and took the time that we submitted the job and the time that the job was done and subtracted the first from the second. Then we average the results:

23:18:19-23:15:59 =3:20
23:22:10-23:19:53 = 3:17
15:38:54-15:33:38 = 5:16

avg = 3:48 min
MapReduce time: to find the time for the MapReduce program to run we collected from the logs the times from all reducers and all mappers, and we divided that time by 4 for the mappers and 3 for the reducers (the number of mappers and reducers we had). 

First job:
Total time spent by all map tasks (ms)=11601915 = 193.36525/4 = 48,25 min
Total time spent by all reduce tasks (ms)=2975197 = 49.586617/3 = 16,5 min

Total time spent by all map tasks (ms)=365519 = 6.091983/4 = 1,5 min
Total time spent by all reduce tasks (ms)=114584 = 1.909733/3 = 0,6 min

Total=66.9min

Second job.
Total time spent by all map tasks (ms)= 12063583 = 201.059717/4 = 50,25
Total time spent by all reduce tasks (ms)= 2455885 = 40.931417/3 = 13,6

Total time spent by all map tasks (ms)= 385291 = 6.421517/4 = 1,6
Total time spent by all reduce tasks (ms)= 110718 = 1.8453/3 = 0,6

Total=66,05min

Third job:
Total time spent by all map tasks (ms)= 11549989 = 192.499817/4 = 48 min
Total time spent by all reduce tasks (ms)= 3271084 = 54.518067/3 = 18,2 min

Total time spent by all map tasks (ms)= 410318 = 6.838633/4 = 1,7 min
Total time spent by all reduce tasks (ms)= 122657 = 2.044283/3 = 0,7 min

Total=68,6min

Avg=67,2min

By looking at the data we can see that the two programs yield the same results with the exception that spark did it in 1/20 of the time MapReduce needed. The reason for that is because the spark is used in-memory processing and in a program that has 2 maps, and 2 reducers spark hasn’t had the bottleneck of shuffle and sort twice as MapReduce had. That makes spark the appropriate tool for the job.


Reference
VICTOR, Friedhelm; WEINTRAUD, Andrea Marie. Detecting and quantifying wash trading on decentralized cryptocurrency exchanges. In: Proceedings of the Web Conference 2021. 2021. p. 23-32.
https://ethereum.org/en/whitepaper/



