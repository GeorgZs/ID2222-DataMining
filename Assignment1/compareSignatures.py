# A class CompareSignatures estimates the similarity of two integer vectors 
# – minhash signatures – as a fraction of components in which they agree.
class CompareSignatures:
    def __init__(self):
        pass

    def compare_signatures(self, sig1, sig2):
        # Calculate similarity of two signatures
        # For each component i, compare sig1[i] and sig2[i]
        # zip function creates a tuple of the ith elements of both lists
        
        result = sum([1 for i, j in zip(sig1, sig2) if i == j]) / len(sig1)
        return result