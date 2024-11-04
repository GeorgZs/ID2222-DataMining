from pyspark.sql import SparkSession
from shingling import Shingling
from minhash import MinHash
from compareSets import CompareSets

def run_simItems(k):
    spark = SparkSession.builder.appName("DocumentSimilarity").getOrCreate()
    sc = spark.sparkContext # spark context used for sending information to the spark cluster
    data_rdd = sc.textFile("data/SMSSpamCollection.txt") # read the data file
    print("Number of lines in data: ", data_rdd.count())

    # CREATE RDD of shingle sets
    newMap = data_rdd.map(lambda line: 
                        tuple(line.split('\t', 1))).mapValues(lambda message: Shingling(k).shingle_document(message))

    # for label, shingles in newMap.take(5):
    #     print(f"Label: {label}, Shingles: {shingles}\n")
    MinHash(newMap).minhash(100)

    # prints 1.0 for all for now
    print("Jaccard = ", CompareSets.jaccard_similarity(newMap.take(3)[0][1], newMap.take(4)[0][1]))

    spark.stop()

if __name__ == "__main__":
    # We currently use 1 document of sentences so shingle size
    # is adapted to words in the sentence
    run_simItems(3)