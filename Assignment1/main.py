from datetime import datetime
from pyspark.sql import SparkSession
from shingling import Shingling
from minhash import MinHash
from compareSets import CompareSets

def run_simItems(k):
    spark = SparkSession.builder.master('local[*]').appName("DocumentSimilarity").getOrCreate()
    sc = spark.sparkContext # spark context used for sending information to the spark cluster
    data_rdd = sc.textFile("data/SMSSpamCollection.txt") # read the data file
    print("Number of lines in data: ", data_rdd.count())

    currTime = datetime.now()
    print(f"CurrTime: {currTime}, Starting SimItems.....")

    # CREATE RDD of shingle sets
    newMap = data_rdd.map(lambda line: 
                        tuple(line.split('\t', 1))).mapValues(lambda message: Shingling(k).shingle_document(message))

    afterShingle = datetime.now() - currTime
    print(f"Time elapsed after shingling (in seconds): {afterShingle}")
    
    sig_matrix = MinHash(newMap).minhash(100)

    afterMinTime = datetime.now() - currTime
    print(f"Time elapsed after MinHash (in seconds): {afterMinTime}")

    # compare signatures
    #print("Jaccard Similarity of [1] and [2]", CompareSets.jaccard_similarity(set(sig_matrix[0]), set(sig_matrix[1])))

    spark.stop()

if __name__ == "__main__":
    # We currently use 1 document of sentences so shingle size
    # is adapted to words in the sentence
    run_simItems(3)