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

        # create matrix with dimensions of each word as row and each shingle as column

        # find all shingle values and create total list (rows)
        listOfShingles = set()
        for _, shingles in self.shingles.collect():
            listOfShingles.update(shingles)

        # I want to create a gloabl set of shingles which will be my rows
        # then each sentence will be the column: therefore we can see if a given sentence
        # contains a shingle and then we assign a 1 to create the matrix
        
        # create signature matrix from shingle values (rows) and shingle sets (columns)
        sig_matrix = np.empty((len(listOfShingles), self.shingles.count()))
        print(sig_matrix.shape)


        for row in range(len(listOfShingles)): # for each row
            for column in range(self.shingles.count()): # for each column

                sig_matrix[row][column] = 0 ## IF we need to show 0 values
                # Add 1 if shingle is in column (look at self.shinglesp[0].getSetItem(row))

                # shingle = ham: {set} so we need to check if row shingle is in the set, if yes we add 1
                # then we should have a matrix with 1 and 0 values 

                if list(listOfShingles)[row] in self.shingles.collect()[column]:
                    sig_matrix[row][column] = 1

        print(sig_matrix)
   
        return
        # Implement minhashing
        pass

    def permute(self, num_hash_functions):
        # Implement permutation
        # we need to have different combinations 

        pass