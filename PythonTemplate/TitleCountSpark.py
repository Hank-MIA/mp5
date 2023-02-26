#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
import re

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

stopWords = set()
with open(stopWordsPath) as f:
    line = f.readline().strip()
    while line:
        stopWords.add(line)
        line = f.readline().strip()

delimiters = []
delimitersReg = ''
with open(delimitersPath) as f:
    line = f.readline().strip()
    while line:
        delimiters.extend([*line])
        line = f.readline().strip()
for d in delimiters:
    delimitersReg += '\\' + d + '|'
delimitersReg = delimitersReg[:-1]

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)
words = lines.flatMap(lambda x: re.split(delimitersReg, x.strip().lower()))
words = words.map(lambda x: x.strip())
words = words.filter(lambda x: x != '' and x not in stopWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y)
topWordCounts = wordCounts.top(10, key=lambda x: x[1])


outputFile = open(sys.argv[4],"w")
for wc in reversed(topWordCounts):
    outputFile.write('%s\t%s\n' % (wc[0], wc[1]))
outputFile.close()

sc.stop()
