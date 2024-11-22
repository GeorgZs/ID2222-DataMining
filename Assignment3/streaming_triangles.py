import random

# Questions:
#   1. Do we wanna avoid duplicate edges?

# Avoid duplicates
# Check the duplicates first
# What if an edge arrives that happens to be the same edge of the wedge

def reservoir_sampling(stream, edge_reservoir_size, wedge_reservoir_size):
    edge_reservoir = []
    wedge_reservoir = []
    wedge_is_closed = [False] * wedge_reservoir_size
    total_wedges = 0
    new_wedges = []

    for time, edge in enumerate(stream, start=1):
        # Check for the closed wedges
        for i, wedge in enumerate(wedge_reservoir):
            if is_closed_by(edge, wedge):
                wedge_is_closed[i] = True

        # Edge reservoir sampling
        if len(edge_reservoir) < edge_reservoir_size:
            edge_reservoir.append(edge)

        else:
            if random.random() < 1 / time:
                index = random.randint(0, edge_reservoir_size - 1)
                edge_reservoir[index] = edge
                total_wedges = update_total_wedges(edge_reservoir)
                new_wedges = generate_new_wedges(edge, edge_reservoir)

        # Wedge reservoir rampling
        for wedge in new_wedges:
            if len(wedge_reservoir) < wedge_reservoir_size:
                wedge_reservoir.append(wedge)

            else:
                if random.random() < len(new_wedges) / total_wedges:
                    index = random.randint(0, wedge_reservoir_size - 1)
                    wedge_reservoir[index] = wedge
                    wedge_is_closed[index] = False
        
        # Check is_closed_by property for previous edges on newly added wedge?

        rho_t = wedge_is_closed.count(True) / wedge_reservoir_size
        kappa_t = 3 * rho_t
        T_t = rho_t * (2 / (edge_reservoir_size * (edge_reservoir_size - 1))) * total_wedges
        # T_t = ((rho_t * time**2) / (edge_reservoir_size * (edge_reservoir_size - 1))) * total_wedges

        # Output running estimates
        print(f"Time {time}: kappa={kappa_t}, T={T_t}")
                


def is_closed_by(edge, wedge):
    # Check whether the two vertices of the edge are part of a wedge.
    # If they are then they form a triangle.
    if edge[0] in wedge and edge[1] in wedge:
        return True
    else:
        return False

def update_total_wedges(edge_reservoir):
    # When a new edge is added we want to change the total number of possible wedges.
    total_wedges = 0

    # Loop over every edge stores and calculate the amount of wedges for every edge
    # We calculate the number of neighbors for every node in the edge so we can
    # sum them afterwards and get the wedges.
    for edge in edge_reservoir:
        neighbors_list = find_neighbors(edge[0], edge_reservoir) + find_neighbors(edge[1], edge_reservoir) 
        total_wedges += len(neighbors_list)

    return total_wedges
        

def generate_new_wedges(edge, edge_reservoir):
    # For every edge in the edge reservoir, generate the wedges and add them
    node_u, node_v = edge
    new_wedges = []
    # We get the neighbors of evey wedge 
    node_u_neighbors = find_neighbors(node_u, edge_reservoir)
    node_v_neighbors = find_neighbors(node_v, edge_reservoir)

    for node_neighbor in node_u_neighbors + node_v_neighbors:
        new_wedges.append((node_u, node_v, node_neighbor))

    return new_wedges
    

def find_neighbors(node, edge_reservoir):
    # For every edge we check whether the vertix of the edge is the node.
    # If it is then we add the other vertix as the neighbor.
    neighbors_list = []
    for edge in edge_reservoir:
        if edge[0] == node:
            neighbors_list.append(edge[1])
        elif edge[1] == node:
            neighbors_list.append(edge[0])

    return neighbors_list