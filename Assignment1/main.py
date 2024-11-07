from datetime import datetime
from pyspark.sql import SparkSession
from shingling import Shingling
from minhash import MinHash
from compareSets import CompareSets
from lsh import LSH

def run_simItems(k, number_bands, rows_per_band):
    spark = SparkSession.builder.master('local[*]').appName("DocumentSimilarity").getOrCreate()
    sc = spark.sparkContext # spark context used for sending information to the spark cluster
    data_rdd = sc.textFile("data/spam.txt") # read the data file
    print("Number of lines in data: ", data_rdd.count())

    currTime = datetime.now()
    print(f"CurrTime: {currTime}, Starting SimItems.....")

    # CREATE RDD of shingle sets
    newMap = data_rdd.map(lambda line: 
                        tuple(line.split('\t', 1))).mapValues(lambda message: Shingling(k).shingle_document(message))

    print(newMap.collect())

    afterShingle = datetime.now() - currTime
    print(f"Time elapsed after shingling (in seconds): {afterShingle}")
    
    # create signature matrix using MinHash
    sig_matrix = MinHash(newMap).minhash(100)

    afterMinTime = datetime.now() - currTime
    print(f"Time elapsed after MinHash (in seconds): {afterMinTime}")

    lsh = LSH(sig_matrix, number_bands, rows_per_band)
    candidates = lsh.compute_candidates()
    print(f"Candidates: {candidates}")

    # compare signatures using Jaccard Similarity
    print("Jaccard Similarity of [1] and [2]", CompareSets.jaccard_similarity(set(sig_matrix[0]), set(sig_matrix[1])))

    # For approximate Jaccard similarity on MinHash signatures
    print("Approximate Jaccard Similarity (MinHash of [1] and [2]):", CompareSets.jaccard_similarity_signatures(sig_matrix, 1, 2))

    finalTime = datetime.now() - currTime
    print(f"Time elapsed after all functions (in seconds): {finalTime}")
    
    # stop the spark session
    spark.stop()

if __name__ == "__main__":
    # We currently use 1 document of sentences so shingle size
    # is adapted to words in the sentence
    run_simItems(3, number_bands=10, rows_per_band=5)