from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("SortedWords")
sc = SparkContext(conf=conf)

def parseLines(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())

lines = sc.textFile("/Users/vidhyasagar/Documents/SparkCourse/Book.txt")
rdd = lines.flatMap(parseLines)

filteredText = rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
reversedKey = filteredText.map(lambda x: (x[1], x[0])).sortByKey()
results = reversedKey.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode("ascii", "ignore")
    if(word):
        print(word.decode()+":\t\t"+count)