Assignment1

    A class Shingling that constructs k–shingles of a given length k (e.g., 10) from a given document, computes a hash value for each unique shingle and represents the document in the form of an ordered set of its hashed k-shingles.
    A class CompareSets computes the Jaccard similarity of two sets of integers – two sets of hashed shingles.
    A class MinHashing that builds a minHash signature (in the form of a vector or a set) of a given length n from a given set of integers (a set of hashed shingles).
    A class CompareSignatures estimates the similarity of two integer vectors – minhash signatures – as a fraction of components in which they agree.
    (Optional task for extra 2 bonus points) A class LSH that implements the LSH technique: given a collection of minhash signatures (integer vectors) and a similarity threshold t, the LSH class (using banding and hashing) finds candidate pairs of signatures agreeing on at least a fraction t of their components.

