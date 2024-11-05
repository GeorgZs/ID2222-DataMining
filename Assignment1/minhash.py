# #A class MinHashing that builds a minHash signature (in the form of a vector or a set) 
# # of a given length n from a given set of integers (a set of hashed shingles).
# import numpy as np

# class MinHash:
#     def __init__(self, shingles):
#         self.shingles = shingles

#     def minhash(self, num_hash_functions):
#         # define common vars
#         shingleCount = self.shingles.count()
#         shingleList = self.shingles.collect()

#         # find all shingle values and create total list (rows)
#         setOfShingles = set()
#         for _, shingles in shingleList:
#             setOfShingles.update(shingles)

#         row_count = len(setOfShingles)
#         column_count = len(shingleList)
#         listOfShingles = list(setOfShingles)

#         # create empty signature matrix with rows = number of shingles and columns = number of docs 
#         sig_matrix = np.empty((row_count, shingleCount))
#         # print(sig_matrix.shape)

#         # create permutation matrix (transverse to see the correct order)
#         # columns = number of hash functions ~ 100
#         # rows = index of shingles in random order ~ 5575
#         permutation = self.permute(num_hash_functions, shingleCount)

#         for row in range(row_count): # for each row
#             curr_shingle = listOfShingles[row]
#             min_index = shingleCount

#             for column in range(column_count): # for each column
#                 # since shingle list is a tuple, we need to check if the shingle is in the list
#                 if curr_shingle in shingleList[column][1]:
#                     # checks each permutation (row) for the smallest index
#                     for perm in permutation:
#                         # print("before min index", min_index)
#                         # print("perm", perm)
#                         min_index = min(min_index, perm[column])
#                         # print("after min index", min_index)
#                         # give sig matrix the smallest index of a permutation                       
#                         sig_matrix[row][column] = min_index
   
#         return sig_matrix

#     def permute(self, num_hash_functions, count):
#         # Implement permutation
#         # we need to have different combinations 
#         permutation = np.empty((num_hash_functions, count))

#         for i in range(num_hash_functions):
#             # create permutation with indexes between 0 and count
#             permute = np.random.permutation(count)
#             # add permutation to the permutation matrix
#             permutation[i] = permute
        
#         return permutation


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
            for row, (a, b) in enumerate(hash_functions):
                # Calculate the minimum hash value for this set of shingles using the hash function
                min_hash = np.inf
                for shingle in shingle_set:
                    hash_val = (a * shingle + b) % max_shingle_id
                    min_hash = min(min_hash, hash_val)
                
                # Update the signature matrix with the minimum hash value for this hash function
                sig_matrix[row][doc_idx] = min_hash

        return sig_matrix

    def permute(self, num_hash_functions, count):
        # Create permutations for hash functions, if needed for other parts of code.
        # Here we use random hash functions, so permute is not needed in this form.
        pass

# Example usage with PySpark RDD:
# Assuming `shingles` is an RDD of (doc_id, shingle_set) where each shingle_set is a set of integers
# shingles = sc.parallelize([(0, {1, 2, 3}), (1, {2, 3, 4}), (2, {1, 4})])
# mh = MinHash(shingles)
# signature_matrix = mh.minhash(100)
