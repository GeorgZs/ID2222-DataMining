from pyspark.sql import SparkSession
from collections import defaultdict
from itertools import chain, combinations
from time import time

K_SIZE = 2
MIN_SUPPORT = 1000
MIN_CONFIDENCE = 0.5
FILENAME = "data/T10I4D100K.dat"
FAKE_FILENAME = "data/Fake.dat"

def main():
    start = time()
    spark = SparkSession.builder.master('local[*]').appName("Spark Apriori").getOrCreate()
    sc = spark.sparkContext

    # Load baskets as RDD
    baskets = sc.textFile(FILENAME).map(lambda line: frozenset(line.strip().split(' ')))

    # Run Apriori using Spark
    all_frequent_itemsets, candidate_counts, item_frequencies = apriori(baskets, K_SIZE, sc)

    print(all_frequent_itemsets)

    #print(type(all_frequent_itemsets))

    # Generate and print association rules
    generate_association_rules(all_frequent_itemsets, item_frequencies, candidate_counts)

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
        # TODO: Understand what the broadcast function does
        broadcast_frequent = spark_context.broadcast(frequent_itemsets)

        # Generate k-candidates and count their occurrences
        candidate_counts = (
            baskets.flatMap(lambda basket: [
                candidate for candidate in generate_k_candidates(basket, broadcast_frequent.value, current_k_size)
            ])
            .map(lambda candidate: (candidate, 1))
            .reduceByKey(lambda count1, count2: count1 + count2)
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
    return all_frequent_itemsets, dict(candidate_counts.collect()), dict(item_frequencies.collect())

#TODO: Add comments for the following functions
def generate_k_candidates(basket, frequent_itemsets, k):
    basket_items = [item for item in frequent_itemsets if item.issubset(basket)]
    return (frozenset(chain.from_iterable(candidate)) for candidate in combinations(basket_items, k))


def generate_association_rules(all_frequent_itemsets, item_counts, candidate_counts):    
    item_counts = item_counts | candidate_counts  # Merge the two dictionaries

    for count, itemsets in all_frequent_itemsets.items():
        if count == 1:
            continue  # Skip singletons
        for itemset in itemsets:
            # Ensure the itemset is correctly formatted (flatten nested frozensets if needed)
            itemset = frozenset(chain.from_iterable(itemset)) if isinstance(next(iter(itemset)), frozenset) else itemset

            # Generate all non-empty subsets of the itemset
            subsets = list(powerset(itemset))

            # Get the full itemset count safely
            full_item_count = item_counts.get(itemset, 0)

            # Iterate over each subset and generate association rules
            for subset in subsets:
                subset = frozenset(subset)

                # Only generate rules if the subset is found in item_counts
                if subset in item_counts:
                    subset_count = item_counts.get(subset, 1)
                    confidence = full_item_count / subset_count if subset_count > 0 else 0

                    # If confidence meets the threshold, generate the rule
                    if confidence >= MIN_CONFIDENCE:
                        remaining_items = itemset - subset
                        if remaining_items:
                            print(subset, "=>", remaining_items, "confidence:", confidence)

def powerset(item):
    # Generate all non-empty subsets of the itemset
    return chain.from_iterable(combinations(item, r) for r in range(1, len(item) + 1))

if __name__ == "__main__":
    main()
