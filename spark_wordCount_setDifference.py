from pyspark import SparkContext, SparkConf
from random import random

# Replacing punctuations with space
def handlePunctuationMarks(data):
    updatedtData = data.replace("{", " ").replace("}", " ").replace(".", " ").replace("*", " ").replace(":", " ").replace("-", " ").replace("!", " ").replace("'", " ").replace(";", " ").replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace("?", " ").replace('"', " ").replace("`", " ").replace(",", " ").replace('/', " ")
    return updatedtData

# Function which returns the word count map for a RDD
def calWordCount(rdd):
    # Not separating words based on punctuations.
    # To handle punctuatuion uncomment following line:
    # return rdd.flatMap(lambda l: handlePunctuationMarks(l[1]).split()).map(lambda w : (w.lower(), 1)).reduceByKey(lambda a, b: a + b).sortByKey()
    return rdd.flatMap(lambda l: l[1].split()).map(lambda w : (w.lower(), 1)).reduceByKey(lambda a, b: a + b).sortByKey()

# Function which returns the Set Difference of R and S. Result is R - S
def calSetDifference(rdd):
    return rdd.flatMap(lambda l: map(lambda x : (x, l[0]), l[1])).groupByKey().mapValues(list).filter(lambda a: len(a[1]) == 1 and a[1][0] == 'R').sortByKey().keys()

if __name__ == "__main__":
    
    sc   = SparkContext.getOrCreate()
    # Word Counts Implementaion
    # Data for Word Count
    dataForWC = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]

    rddForWC = sc.parallelize(dataForWC)
    resForWC = calWordCount(rddForWC).collect()
    print("\n\n********** Word Count Result *****************\n")
    print(resForWC)
    print("\n\n")

    # Set Difference Implementaion
    # Data for Set Difference
    data1ForSetDifference = [('R', ['apple', 'orange', 'pear', 'blueberry']),
                             ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2ForSetDifference = [('R', [x for x in range(50) if random() > 0.5]),
                             ('S', [x for x in range(50) if random() > 0.75])]
    rdd1ForSetDifference = sc.parallelize(data1ForSetDifference)
    res1ForSetDifference = calSetDifference(rdd1ForSetDifference).collect()
    print("\n\n********** Set Difference 1 Result -> R - S *****************\n")
    print(res1ForSetDifference)
    print("\n\n")
    
    rdd2ForSetDifference = sc.parallelize(data2ForSetDifference)
    res2ForSetDifference = calSetDifference(rdd2ForSetDifference).collect()
    print("\n\n********** Set Difference 2 Result -> R - S *****************\n")
    print(res2ForSetDifference)
    print("\n\n")

