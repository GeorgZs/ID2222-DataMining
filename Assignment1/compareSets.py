import numpy as np
from decimal import Decimal, getcontext

class CompareSets:
    def __init__(self):
        self

    # Return the accurate Jaccard similarity between two sets
    def jaccard_similarity(set1, set2):
        return len(set1.intersection(set2)) / len(set1.union(set2))
    
    # CHANGES: If we want to calculate Jaccard similarity for the entire set
    # it would be computationally very expensive. Therefore, we create this 
    # method that compares the signatures derived from every hash function.
    #
    # THE IDEA: If two sets are similar, their MinHash signatures will also 
    # be similar. This way we look at the number of hash functions that give
    # the same result for both sets. Counting how many hash functions agree
    # and output the proportion of those, simulating Jaccard Similarity.
    def jaccard_similarity_signatures(sig_matrix, doc1_idx, doc2_idx):
        sig1 = sig_matrix[:, doc1_idx]
        print("Sig1: ", sig1)
        sig2 = sig_matrix[:, doc2_idx]
        print("Sig2: ", sig2)
        
        # We perhaps need a way to get the rest of the decimal values
        match_count = np.sum(sig1 == sig2)  # This counts how many positions match
        print("Match count: ", match_count)
        total_count = len(sig1)  # Total number of elements in the signature
        print("Total count: ", total_count)
        
        # Calculate similarity as the fraction of matching elements
        similarity = match_count / total_count
        
        # Return the similarity value as a Decimal for higher precision
        return similarity
        