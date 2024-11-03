class CompareSets:
    def __init__(self):
        pass

    def jaccard_similarity(self, set1, set2):
        # Calculate Jaccard similarity
        return len(set1.intersection(set2)) / len(set1.union(set2))