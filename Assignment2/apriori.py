from collections import defaultdict
from itertools import chain, combinations

K_SIZE = 2
MIN_SUPPORT = 2
MIN_CONFIDENCE = 0.6
FILENAME = "data/T10I4D100K.dat"
FAKE_FILENAME = "data/Fake.dat"

def main():
    baskets = load_baskets(FAKE_FILENAME)
    all_frequent_itemsets, item_counts = apriori(baskets, K_SIZE)

    # generate and print association rules
    generate_association_rules(all_frequent_itemsets, item_counts)
    

def load_baskets(filename):
    baskets = []
    with open(filename, "r") as file:
        for line in file:
            basket = [int(item) for item in line.strip().split(' ')]
            baskets.append(basket)

    return baskets

def apriori(baskets, k):
    item_counts = defaultdict(int)
    for basket in baskets:
        for item in basket:
            item_counts[frozenset([item])] += 1

    frequent_itemsets = set()
    for itemset, count in item_counts.items():
        if count >= MIN_SUPPORT:
            frequent_itemsets.add(itemset)

    all_frequent_itemsets = {1: frequent_itemsets}

    current_k_size = k
    while len(frequent_itemsets) > 0:
        candidates = generate_k_candidates(frequent_itemsets, current_k_size)
        frequent_itemsets = count_support(baskets, candidates, item_counts)
        if len(frequent_itemsets) > 0:
            all_frequent_itemsets[current_k_size] = frequent_itemsets
        
        current_k_size += 1

    return all_frequent_itemsets, item_counts


def generate_k_candidates(frequent_itemset, k):
    candidates = set()
    for itemset1 in frequent_itemset:
        for itemset2 in frequent_itemset:
            candidate = itemset1.union(itemset2)
            if len(candidate) == k:
                candidates.add(frozenset(candidate))

    return candidates

def count_support(baskets, candidates, item_counts):
    itemset_counts = defaultdict(int)
    for basket in baskets:
        basket_set = frozenset(basket)
        for candidate in candidates:
            if candidate.issubset(basket_set):
                itemset_counts[candidate] += 1

    frequent_itemsets = set()
    for itemset, count in itemset_counts.items():
        if count >= MIN_SUPPORT:
            frequent_itemsets.add(itemset)
            item_counts[itemset] = count  # Store count of each frequent itemset NOT just singletons

    return frequent_itemsets

def generate_association_rules(all_frequent_itemsets, item_counts):
    for count, itemsets in all_frequent_itemsets.items():
        if count == 1:
            continue # skip singletons as no association rules exist
        for item in itemsets:
            subsets = powerset(item)
            full_item = item_counts[item] # count of the current itemset

            for subset in subsets:
                subset = frozenset(subset)
                if(subset in item_counts):
                    subset_support = item_counts[subset]  # count of current subset
                    confidence = float(full_item / subset_support) # union count / subset count
                    # print confidence if it meets the minimum confidence threshold
                    if confidence >= MIN_CONFIDENCE:
                        print(subset, "=>", item, "confidence:", confidence)  
     

# Creates all possible combianations of dataset for association rule
# using recursive combinations function from itertools
def powerset(item):
    return chain.from_iterable(combinations(item, r) for r in range(1, len(item)))   

if __name__ == "__main__":
    main()