#A class MinHashing that builds a minHash signature (in the form of a vector or a set) 
# of a given length n from a given set of integers (a set of hashed shingles).
import numpy as np

class MinHash:
    def __init__(self, shingles):
        self.shingles = shingles

    def minhash(self, num_hash_functions):
        # Implement minhashing
        #
        # S1 = {a, b}, S2 = {a, c} S3 = {a,b,c,d}
        #  sets of shingles and occurence of shingles in the sets
        # | |S1|S2|S3|
        # |-| -| -| -|
        # |a| 1| 1| 1|
        # |b| 1| 0| 1|
        # |c| 0| 1| 1|
        # |d| 0| 0| 1|
        # OR...
        # [[S1, a, 1], [S1, b, 1], [S2, a, 1], [S2, c, 1], [S3, a, 1], [S3, b, 1], [S3, c, 1], [S3, d, 1]]

        # define common vars
        shingleCount = self.shingles.count()
        shingleList = self.shingles.collect()

        # find all shingle values and create total list (rows)
        setOfShingles = set()
        for _, shingles in shingleList:
            setOfShingles.update(shingles)

        row_count = len(setOfShingles)
        column_count = len(shingleList)
        listOfShingles = list(setOfShingles)

        # create empty signature matrix with rows = number of shingles and columns = number of docs 
        sig_matrix = np.empty((row_count, shingleCount))
        # print(sig_matrix.shape)

        # create permutation matrix (transverse to see the correct order)
        # columns = number of hash functions ~ 100
        # rows = index of shingles in random order ~ 5575
        permutation = self.permute(num_hash_functions, shingleCount)

        # permuation structure
        # | |perm1|perm2|perm3| ... until num_hash_functions
        # |-| -   | -   | -   |
        # | |    1|    1|    1|
        # | |    4|    0|    9|
        # | |    8|    1|    3|
        # | |    3|    6|    1|
        # .... until column_count

        for row in range(row_count): # for each row
            curr_shingle = listOfShingles[row]
            min_index = shingleCount

            for column in range(column_count): # for each column
                # since shingle list is a tuple, we need to check if the shingle is in the list
                if curr_shingle in shingleList[column][1]:
                    # checks each permutation (row) for the smallest index
                    for perm in permutation:
                        # print("before min index", min_index)
                        # print("perm", perm)
                        min_index = min(min_index, perm[column])
                        # print("after min index", min_index)
                        # give sig matrix the smallest index of a permutation                       
                        sig_matrix[row][column] = min_index
   
        return sig_matrix

    def permute(self, num_hash_functions, count):
        # Implement permutation
        # we need to have different combinations 
        permutation = np.empty((num_hash_functions, count))

        for i in range(num_hash_functions):
            # create permutation with indexes between 0 and count
            permute = np.random.permutation(count)
            # add permutation to the permutation matrix
            permutation[i] = permute
        
        return permutation