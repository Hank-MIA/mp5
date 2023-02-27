#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 
lines = lines.map(lambda l: l.strip()).filter(lambda l: l!= '')


def splitAndMap(l):
    listOfTuples = []
    parent, children = l.split(':')
    parent = parent.strip()
    for child in children:
        c = child.strip()
        if c != '' and c != parent:
            listOfTuples.append((c, 'n'))
    listOfTuples.append((parent, 'p'))
    return listOfTuples


orpRec = lines.flatMap(lambda l: splitAndMap(l))
orpRecCompressed = orpRec.reduceByKey(lambda x,y: 'n' if x == 'n' or y == 'n' else 'p')
orphans = orpRecCompressed.filter(lambda x: x[1] == 'p').map(lambda x: x[0])
sortedOrphans = orphans.sortBy(lambda x: x).collect()

output = open(sys.argv[2], "w")
for o in sortedOrphans:
    output.write('%s\n' % o)
output.close()

sc.stop()

