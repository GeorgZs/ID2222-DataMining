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

def laplacianMatrix(affinity_matrix):
    # calculate the diagonal matrix
    D = np.diag(np.sum(affinity_matrix, axis=1))
    # normalize the diagonal matrix using sqrt and inverting it
    D_inv = np.linalg.inv(np.sqrt(D))
    # compute symmetric normalized Laplacian using (without identify matrix):
    # I - L_sym =  D^(-1/2) * A * D^(-1/2)
    return D_inv @ affinity_matrix @ D_inv

def computeK(eigen_values):
    # calculate the difference between the eigen values
    eigen_values_diff = np.diff(eigen_values)
    # get the index of the largest difference
    index_largest_diff = np.argmax(eigen_values_diff) + 1
    # return the number of clusters based on the eigengap heuristic.
    return len(eigen_values) - index_largest_diff

def computeEigenvectors(laplacian_matrix, image_path):
    # Compute eigenvalues and eigenvectors::
    # eigh() = Solve a standard or generalized eigenvalue problem 
    # for a complex Hermitian or real symmetric matrix.
    eigenvalues, eigenvectors = la.eigh(laplacian_matrix)
    # compute k so that we can get the k largest eigenvectors
    k = computeK(eigenvalues)

    # Plot the eigenvectors up to k+2
    plt.figure()  # Create a new figure
    for i in range(1, k+2):
        plt.plot(sorted(eigenvectors[:, -i]))
    plt.title("Eigenvectors")
    new_path = image_path + 'eigenvectors.png'
    plt.savefig(new_path)

    # Get the 2nd smallest eigenvectors to get the fielder vector
    fiedler_vec = sorted(eigenvectors[:, -2])
    # get the k largest eigenvectors
    k_largest_eigen = eigenvectors[:, -k:]

    return k_largest_eigen, k, eigenvalues, fiedler_vec

def normalized_eigen(k_largest_eigen):
    # Y (n x k)
    # Y = X_ij / (Sum_j (X_ij ^ 2)) ^(1/2)
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