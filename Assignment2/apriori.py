from pyspark.sql import SparkSession
from collections import defaultdict
from itertools import chain, combinations
from time import time

K_SIZE = 2
MIN_SUPPORT = 2
MIN_CONFIDENCE = 0.6
FILENAME = "data/T10I4D100K.dat"
FAKE_FILENAME = "data/simple.dat"

def main():
    start = time()
    spark = SparkSession.builder.master('local[*]').appName("Spark Apriori").getOrCreate()
    sc = spark.sparkContext

    # Load baskets as RDD
    baskets = sc.textFile(FAKE_FILENAME).map(lambda line: frozenset(line.strip().split(' ')))

    # Run Apriori using Spark
    all_frequent_itemsets, item_counts = apriori(baskets, K_SIZE, sc)

    print(all_frequent_itemsets)

    # Generate and print association rules
    generate_association_rules(all_frequent_itemsets, item_counts)

    print("Total execution time: ", time() - start)

def apriori(baskets, max_k, spark_context):
    # Count single-item frequencies
    item_frequencies = (
        baskets.flatMap(lambda basket: [(frozenset([item]), 1) for item in basket])  # Create pairs for each item
               .reduceByKey(lambda count1, count2: count1 + count2)  # Sum up the counts for each item
    )
    
    # Filter out items that do not meet the minimum support threshold
    frequent_itemsets = (
        item_frequencies.filter(lambda item_frequency: item_frequency[1] >= MIN_SUPPORT)
                        .keys()  # Extract only the keys (the itemsets themselves)
                        .collect()
    )
    
    # Initialize the dictionary to store all frequent itemsets, grouped by size
    all_frequent_itemsets = {1: set(frequent_itemsets)}  # Store frequent single items
    current_k_size = 2  # Start with pairs

    while frequent_itemsets:
        # Generate k-candidates
        broadcast_frequent = spark_context.broadcast(frequent_itemsets)  # Share frequent itemsets across workers
        candidate_counts = (
            baskets.flatMap(lambda basket: [
                candidate for candidate in generate_k_candidates(basket, broadcast_frequent.value, current_k_size)
            ])
            .map(lambda candidate: (candidate, 1))  # Create pairs for counting
            .reduceByKey(lambda count1, count2: count1 + count2)  # Sum up counts for each candidate
        )
        
        # Filter candidates to find frequent itemsets
        frequent_itemsets = (
            candidate_counts.filter(lambda candidate_frequency: candidate_frequency[1] >= MIN_SUPPORT)
                            .keys()  # Extract the frequent itemsets
                            .collect()
        )
        
        # Add the frequent itemsets of this size to the dictionary
        if frequent_itemsets:
            all_frequent_itemsets[current_k_size] = set(frequent_itemsets)
        
        current_k_size += 1
        if current_k_size > max_k:  # Stop if the desired maximum size is reached
            break

    # Return all frequent itemsets and the full dictionary of counts for candidates
    return all_frequent_itemsets, dict(candidate_counts.collect())


def generate_k_candidates(basket, frequent_itemsets, k):
    basket_items = [item for item in frequent_itemsets if item.issubset(basket)]
    return (frozenset(candidate) for candidate in combinations(basket_items, k))

def generate_association_rules(all_frequent_itemsets, item_counts):
    for count, itemsets in all_frequent_itemsets.items():
        if count == 1:
            continue  # Skip singletons
        for itemset in itemsets:
            subsets = powerset(itemset)
            full_item_count = item_counts[itemset]
            for subset in subsets:
                subset = frozenset(subset)
                if subset in item_counts:
                    confidence = full_item_count / item_counts[subset]
                    if confidence >= MIN_CONFIDENCE:
                        print(subset, "=>", itemset - subset, "confidence:", confidence)

def powerset(item):
    return chain.from_iterable(combinations(item, r) for r in range(1, len(item)))

if __name__ == "__main__":
    main()
