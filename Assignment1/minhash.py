import numpy as np

class MinHash:
    def __init__(self, shingles):
        # Assuming `shingles` is an RDD of (doc_id, shingle_set) pairs
        self.shingles = shingles
        self.num_docs = self.shingles.count()
        self.shingle_list = self.shingles.collect()

    def minhash(self, num_hash_functions):
        # Identify the largest shingle ID for use in hash functions
        max_shingle_id = max([max(shingle_set) for _, shingle_set in self.shingle_list if shingle_set])
        
        # Generate a list of random hash functions as (a, b) pairs
        hash_functions = [
            # (a with value 1 - (max_shingle_id - 1), b with values 0 - (max_shingle_id - 1)) pairs for hash functions
            (np.random.randint(1, max_shingle_id), np.random.randint(0, max_shingle_id))
            # creates this pair for num_hash_functions times using list comprehension
            for _ in range(num_hash_functions)
        ]
        
        # Initialize signature matrix with infinity values
        sig_matrix = np.full((num_hash_functions, self.num_docs), np.inf)

        # Loop through each document's shingles to calculate minhash values
        for doc_idx, (_, shingle_set) in enumerate(self.shingle_list):
            # Loop through each hash function to calculate minhash values
            for row, (a, b) in enumerate(hash_functions):
                # Calculate the minimum hash value for this set of shingles using the hash function
                min_hash = np.inf
                for shingle in shingle_set:
                    # Create hash value based on curr_shingle, a, b, and max_shingle_id
                    hash_val = (a * shingle + b) % max_shingle_id
                    # Update min_hash if hash_val is smaller
                    min_hash = min(min_hash, hash_val)
                
                # Update the signature matrix with the minimum hash value for this hash function
                sig_matrix[row][doc_idx] = min_hash

        return sig_matrix