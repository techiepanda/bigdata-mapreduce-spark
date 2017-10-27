from pyspark import SparkContext, SparkConf
import sys

# Returns the key-value pairs with key as industry name and values as list of tuples with date and frequencies
def getFrquencyMap(fileContent):
    # Splitting data based on date and post tags
    dateList = fileContent.split("</date>")
    postList = fileContent.split("</post>")
    res = dict()
    for i, dateData in enumerate(dateList):
        if "<date>" in dateData:
            postData = postList[i]
            # Searching post data for corresponding date tag
            if "<post>" in postData:
                dateString = dateData[dateData.find("<date>")+ len("<date>"):]
                dateArray = dateString.split(",");
                actualDate = dateArray[-1] + "-" + dateArray[-2]  # Storing only year and month in format: (year-month)
                
                postString = postData[postData.find("<post>")+ len("<post>"):].lower()
                # Replacing punctuations with spaces
                postString = postString.replace("{", " ").replace("}", " ").replace(".", " ").replace("*", " ").replace(":", " ").replace("-", " ").replace("!", " ").replace("'", " ").replace(";", " ").replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ").replace("?", " ").replace('"', " ").replace("`", " ").replace(",", " ").replace('/', " ")
                for industry in industriesBC.value:   # Search for industry names in data
                    count = postString.split(" ").count(industry.lower())  # Split is needed so that it can count exact words
                    if count > 0:
                        try:
                            if res[industry]:
                                index = 0   # Used to keep track of index of entry need to be updated
                                found = 0
                                for x in res[industry]:
                                    if (x[0] == actualDate):
                                        res[industry][index] = (actualDate, res[industry][index][1] + count)  # Update count for corresponding matched index
                                        found = 1
                                    else:
                                        index += 1
                                        
                                if found == 0:
                                    res[industry].append((actualDate, count));
                        except KeyError:
                            res[industry] = [(actualDate, count)];
    # Output returned is in this format : {'indName1': [('Y1-M1', CombinedFreq)], 'indName2': [('Y2-M2', CombinedFreq),('Y3-M3', CombinedFreq)]},
    return res

# Reduce the values and return combined tuple list
def reduceData(a, b):
    dict1 = dict(a)
    dict2 = dict(b)
    # Joining both lists and adding values for same dates.
    res = {x: dict1.get(x, 0) + dict2.get(x, 0) for x in set(dict1).union(dict2)}
    # Returning tuple of tuples with date and frequencies
    return tuple([(k, v) for k, v in res.items()])

if __name__ == "__main__":
 
    sc   = SparkContext.getOrCreate()
    dirPath = '/Users/*'  # Dummy Path. Pass directory path as an argument through command line
    if len(sys.argv) > 1:
        dirPath = sys.argv[1]   # Reading directory path from command line
    filesRDD = sc.wholeTextFiles(dirPath)  # Reading file from directory
    # Fetching list of available industries. Encoding is required to remove the extra unicode character in file names
    industriesList = filesRDD.map(lambda x:x[0].split(".")[3].encode('ascii','ignore').decode('UTF-8')).collect()
    industriesBC = sc.broadcast(set(industriesList))
    print("\n\n*****************\n Industries List:\n*****************\n")
    print(industriesBC.value)
    # Getting frequencies of industries in files
    res = filesRDD.values().map(lambda p: getFrquencyMap(p)).flatMap(lambda l:l.items()).reduceByKey(lambda a,b: reduceData(a,b)).collect()
    print("\n\n*****************\n Frequencies of Industries:\n*****************\n")
    print(res)

