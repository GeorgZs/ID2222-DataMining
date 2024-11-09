# A class Shingling that constructs k–shingles of a given length k (e.g., 10) from a given document, 
# computes a hash value for each unique shingle and represents the 
# document in the form of an ordered set of its hashed k-shingles.

class Shingling:
    def __init__(self, k):
        self.k = k

    def shingle_document(self, document):
        shingles = set()

        # split the document into words
        words = document.split()

        for i in range(len(words) - self.k + 1):
            # Construct k-shingle as a string from shingle list ==> turn ["h","b","c"] into "h b c"
            shingle = ' '.join(words[i:i + self.k])  
            #hashed_shingle = hash(shingle)  # Hash the shingle
            shingles.add(hash(shingle))
            
            # shingles.add(hashed_shingle)

        return shingles