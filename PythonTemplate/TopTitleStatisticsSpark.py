#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
import math

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

lines = lines.map(lambda line: line.strip()).filter(lambda line: line != '')
counts = lines.map(lambda line: int(line.split('\t')[1]))
ans1 = counts.mean()
ans2 = counts.sum()
ans3 = counts.min()
ans4 = counts.max()
ans5 = counts.variance()
outputFile = open(sys.argv[2], "w")
outputFile.write('Mean\t%s\n' % math.floor(ans1))
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % math.floor(ans5))
outputFile.close()

sc.stop()

