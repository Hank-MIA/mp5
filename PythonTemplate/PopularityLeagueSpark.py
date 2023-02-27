#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

leagueIds = sc.textFile(sys.argv[2], 1)
leagueIds = leagueIds.map(lambda x: x.strip()).filter(lambda x: x != '')
league = sc.broadcast(set(leagueIds.collect()))


def splitAndMap(l):
    kvs = []
    links = (l.split(':')[1]).split(' ')
    for link in links:
        li = link.strip()
        if li != '' and li in league.value:
            kvs.append((li, 1))
    return kvs


linkStream = lines.flatMap(lambda l: splitAndMap(l))
linkCounts = linkStream.reduceByKey(lambda x,y: x+y)
linkCountsList = linkCounts.collect()
linkCountsList.sort()

output = open(sys.argv[3], "w")
for lc in linkCountsList:
    output.write('%s\t%s\n' % (lc[0], lc[1]))
output.close()


#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

