# (Optional task for extra 2 bonus points) A class LSH that implements the LSH technique: 
# given a collection of minhash signatures (integer vectors) and a similarity threshold t, 
# the LSH class (using banding and hashing) finds candidate pairs of 
# signatures agreeing on at least a fraction t of their components.

import numpy as np
from collections import defaultdict
from compareSignatures import CompareSignatures

class LSH:
    def __init__(self, signature_matrix, number_bands, rows_per_band):
        self.signature_matrix = signature_matrix
        self.number_bands = number_bands
        self.rows_per_band = rows_per_band
        self.number_docs = signature_matrix.shape[1]

        self.threshold = (1 / self.number_bands) ** (1 / self.rows_per_band)
        print("Threshold is set to:", self.threshold)
        

    def compute_candidates(self):
        candidate_pairs = set()

        # For every band we create a bucket for the amount of rows specified
        for band in range(self.number_bands):
            band_buckets = defaultdict(list)
            start_row = band * self.rows_per_band # Start row of the band based on index (index starts at 0 so it has band 0, 1, index 1 with have bands 2, 3, etc.)
            end_row = start_row + self.rows_per_band

            # For every document, we take the signature from the start row, to the end row
            # Then we fill up the bucket for that key/tuple. Example of the tuple: 
            # (0, 1, 3): [2], [4] with that specfic document index
            # because that hash happens in that document. If the band does not exist it will
            # make a new one, otherwise it will add it to the already existing one.
            for document_index in range(self.number_docs):
                band_signature = tuple(self.signature_matrix[start_row:end_row, document_index])
                band_buckets[band_signature].append(document_index)

            # This method goes over every bucket and just makes pairs out of the values in the dictionary
            # which are all of the doucments that share that specific hash
            for bucket in band_buckets.values():
                if len(bucket) > 1:
                    for i in range(len(bucket)):
                        for j in range(i + 1, len(bucket)):
                            candidate_pairs.add((bucket[i], bucket[j]))

        #print("Candidate pairs: ", candidate_pairs)
        final_candidates = set()

        for(doc1, doc2) in candidate_pairs:

            similarity = CompareSignatures.compare_signatures(
                CompareSignatures,
                self.signature_matrix[:, doc1],
                self.signature_matrix[:, doc2]
            )
            print("Comparing documents: ", doc1, " and ", doc2, ". Similarity of ", similarity)

            if similarity >= self.threshold:
                final_candidates.add((doc1, doc2))

        return final_candidates