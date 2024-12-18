import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
from networkx import DiGraph
from sklearn.cluster import KMeans
import scipy.linalg as la

def load_graph(path):
    if path == "data/example1.dat":
        graph = nx.read_edgelist(path, delimiter=",", create_using=DiGraph)
    else:
        # read the weighted graph in example2.dat
        graph = nx.read_weighted_edgelist(path, delimiter=",", create_using=DiGraph)

    return graph

def plot_graph_without_connections(graph, labels, pos, image_path):
    plt.figure()  # Create a new figure
    plt.title("Clustered graph for current data set")
   
    nx.draw_networkx_nodes(graph, pos=pos, node_size=6, 
                           node_color=labels, cmap=plt.cm.Set1)
    
    new_path = image_path + 'plot_graph_without_conn.png'
    plt.savefig(new_path)

def plot_graph_with_connections(graph, labels, pos, image_path):
    plt.figure()  # Create a new figure
    plt.title("Clustered graph for current data set")
    nx.draw_networkx(graph, pos=pos, node_size=6, node_color=labels,
                           cmap=plt.cm.Set1, with_labels=False)
    new_path = image_path + 'plot_graph_with_conn.png'
    plt.savefig(new_path)

def plot_eigen_values_diff(eig_values, image_path):
    plt.figure()  # Create a new figure
    plt.scatter(range(len(eig_values)), eig_values)
    plt.title("Biggest eigen values difference to find K-clusters (eigengap representation)")
    new_path = image_path + 'plot_eigen_values_diff.png'
    plt.savefig(new_path)

def plot_fiedler(fiedler_vec, image_path):
    plt.figure()  # Create a new figure
    plt.plot(range(len(fiedler_vec)), fiedler_vec)
    plt.title("Sorted Fiedler Vector")
    new_path = image_path + 'plot_fiedler.png'
    plt.savefig(new_path)

def plot_sparse(A, image_path):
    plt.figure()  # Create a new figure
    plt.title("Show sparsity distribution of Affinity Matrix")
    plt.imshow(A, cmap='Blues', interpolation='nearest')
    new_path = image_path + 'plot_sprase.png'
    plt.savefig(new_path)

def affinityMatrix(graph):
    # A = N x N matrix
    # A_ii = 0
    # A_ij = exp(-||x_i - x_j||^2 / 2 * sigma^2)
    return np.asarray(nx.adjacency_matrix(graph).todense()) 
    # The adjacency matrix represents the connectivity of the graph.
    # Each element A_ij is 1 if there's an edge between nodes i and j, and 0 otherwise.

def laplacianMatrix(affinity_matrix):
    # calculate the diagonal matrix
    D = np.diag(np.sum(affinity_matrix, axis=1))
    # normalize the diagonal matrix using sqrt and inverting it
    D_inv = np.linalg.inv(np.sqrt(D))
    # compute symmetric normalized Laplacian using (without identify matrix):
    # I - L_sym =  D^(-1/2) * A * D^(-1/2)
    return D_inv @ affinity_matrix @ D_inv

    # laplacian = diveragance of the gradient of a function
    # encodes the structure and connectivity of the graph
    # used for eigendecomposition to find the number of clusters (dimensionality reduction)
    # to find clustering using eigenvalues found from the laplacian matrix

    # The Laplacian matrix L has an eigenvalue 0 with the multiplicity k
    # iff the graph has k connecgted components

def computeK(eigen_values):
    # calculate the difference between the eigen values
    eigen_values_diff = np.diff(eigen_values)
    # get the index of the largest difference
    index_largest_diff = np.argmax(eigen_values_diff) + 1
    # return the number of clusters based on the eigengap heuristic.
    return len(eigen_values) - index_largest_diff
    
    # The eigengap heuristic suggests that the number of clusters
    # is equal to the number of eigenvalues before the largest gap
    # in the sorted eigenvalue sequence.
    # We subtract the index of the largest gap from the total number
    # of eigenvalues because we're working with the largest eigenvalues
    # (the eigenvalues are typically sorted in ascending order).

def computeEigenvectors(laplacian_matrix, image_path):
    # Compute eigenvalues and eigenvectors::
    # eigh() = Solve a standard or generalized eigenvalue problem 
    # for a complex Hermitian or real symmetric matrix.
    eigenvalues, eigenvectors = la.eigh(laplacian_matrix)
    # compute k so that we can get the k largest eigenvectors
    k = computeK(eigenvalues)

   # The plot shows the (k+2 since we start at 1) k+1 largest eigenvectors, which helps visualize
    # the structure in the data and can provide insights into the number
    # of clusters and the quality of the spectral clustering.
    plt.figure()  # Create a new figure
    for i in range(1, k+2):
        plt.plot(sorted(eigenvectors[:, -i]))
    plt.title("Eigenvectors")
    new_path = image_path + 'eigenvectors.png'
    plt.savefig(new_path)

    # Get the 2nd smallest eigenvectors to get the fielder vector 
    # (corresponding to the eigenvalue immediately after zero)
    # sorted in ascending order to created sorted fiedler graph and show clusterings
    fiedler_vec = sorted(eigenvectors[:, -2])
    # get the k largest eigenvectors
    k_largest_eigen = eigenvectors[:, -k:]

    return k_largest_eigen, k, eigenvalues, fiedler_vec

def normalized_eigen(k_largest_eigen):
    # Y (n x k)
    # Y = X_ij / (Sum_j (X_ij ^ 2)) ^(1/2)
    # Normalization ensures that each dimension (eigenvector) 
    # contributes equally to the clustering process, preventing any single 
    # dimension from dominating due to its scale

    # after sqrt, we have a 1D array that needs to be reshaped to from (n,) to (n, 1)
    # for which we can then easily fit this matrix into the K_means algorithm
    return k_largest_eigen / np.sqrt(np.sum(k_largest_eigen ** 2, axis = 1)).reshape((-1, 1))

def main():
    path = "./data/example2.dat"
    image_path = "./pics/example2/"
    graph = load_graph(path)
    print("Number of nodes: ", len(graph.nodes))
    pos = nx.spring_layout(graph)
    A_matrix = affinityMatrix(graph)
    L_matrix = laplacianMatrix(A_matrix)
    k_max_eigen, k, evalues, fiedler_vec = computeEigenvectors(L_matrix, image_path)
    norm_eigen = normalized_eigen(k_max_eigen)
    # APPLY K-MEANS clustering to normalized k_largest_eigenvectors
    clustering = KMeans(n_clusters=k).fit(norm_eigen)
    labels = clustering.labels_
    plot_graph_without_connections(graph, labels, pos, image_path)
    plot_graph_with_connections(graph, labels, pos, image_path)
    plot_eigen_values_diff(evalues, image_path)
    plot_fiedler(fiedler_vec, image_path)
    plot_sparse(A_matrix, image_path)

if __name__ == '__main__':
    main()