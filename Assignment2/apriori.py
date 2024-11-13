from collections import defaultdict

K_SIZE = 2
MIN_SUPPORT = 2
MIN_CONFIDENCE = 0.6
FILENAME = "data/T10I4D100K.dat"

def main():
    baskets = load_baskets(FILENAME)
    all_frequent_itemsets = apriori(baskets, K_SIZE)
    print(all_frequent_itemsets)


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
        frequent_itemsets = count_support(baskets, candidates)
        if len(frequent_itemsets) > 0:
            all_frequent_itemsets[current_k_size] = frequent_itemsets
        
        current_k_size += 1

    return all_frequent_itemsets


def generate_k_candidates(frequent_itemset, k):
    candidates = set()
    for itemset1 in frequent_itemset:
        for itemset2 in frequent_itemset:
            candidate = itemset1.union(itemset2)
            if len(candidate) == k:
                candidates.add(frozenset(candidate))

    return candidates

def load_baskets(filename):
    baskets = []
    with open(filename, "r") as file:
        for line in file:
            basket = [int(item) for item in line.strip().split(' ')]
            baskets.append(basket)

    return baskets

def count_support(baskets, candidates):
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

    return frequent_itemsets

        
if __name__ == "__main__":
    main()