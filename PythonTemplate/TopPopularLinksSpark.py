#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)


def splitAndMap(l):
    links = (l.split(':')[1]).strip().split(' ')
    kvs = []
    for link in links:
        li = link.strip()
        if li != '':
            kvs.append((li, 1))
    return kvs


linkCountPairs = lines.flatMap(lambda l: splitAndMap(l))
linkCount = linkCountPairs.reduceByKey(lambda x, y: x+y)
orderedLinkCount = linkCount.top(10, key=lambda x: (x[1], x[0]))
orderedLinkCount.sort()

output = open(sys.argv[2], "w")
for kv in orderedLinkCount:
    output.write('%s\t%s\n' % (kv[0], kv[1]))
output.close()

#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

