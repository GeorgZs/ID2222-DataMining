import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, expr
from pyspark.sql.types import ArrayType, IntegerType

# Questions:
#   1. Do we wanna avoid duplicate edges?

# Avoid duplicates 
# Check the duplicates first ✓
# What if an edge arrives that happens to be the same edge of the wedge

# Initialize Spark session
spark = SparkSession.builder.appName("TriangleCountingReservoirSampling").getOrCreate()


def reservoir_sampling(stream, edge_reservoir_size, wedge_reservoir_size):
    edge_reservoir = set()
    wedge_reservoir = set()
    wedge_is_closed = [False] * wedge_reservoir_size
    total_wedges = 0
    new_wedges = [] # TODO: set or not??

    for time, edge in enumerate(stream, start=1):
        # Check whether edge is already in the reservoir to avoid duplicates
        # TODO: oscar check if this is correct (edge vs wedge reservoir here)
        if edge not in wedge_reservoir:
            # Check for the closed wedges
            for i, wedge in enumerate(wedge_reservoir):
                if is_closed_by(edge, wedge, wedge_reservoir):
                    wedge_is_closed[i] = True

        # Edge reservoir sampling
        if len(edge_reservoir) < edge_reservoir_size:
            edge_reservoir.append(edge)

        else:
            if random.random() < 1 / time:
                index = random.randint(0, edge_reservoir_size - 1)
                edge_reservoir.add(edge) 
                total_wedges = update_total_wedges(edge_reservoir)
                new_wedges = generate_new_wedges(edge, edge_reservoir)

        # Wedge reservoir rampling
        for wedge in new_wedges:
            if len(wedge_reservoir) < wedge_reservoir_size:
                wedge_reservoir.add(wedge)

            else:
                if random.random() < len(new_wedges) / total_wedges:
                    index = random.randint(0, wedge_reservoir_size - 1)
                    wedge_reservoir.add(wedge)
                    wedge_is_closed[index] = False
        
        # Check is_closed_by property for previous edges on newly added wedge?

            # Make sure to re-run new wedge reservoir on previous edges
            if len(wedge_reservoir) == wedge_reservoir_size:
                for i, curr_edge in enumerate(edge_reservoir): # complexity ???? O(stream_size * new_wedges * edge_reservoir)
                    if is_closed_by(curr_edge, wedge):
                        wedge_is_closed[i] = True

        rho_t = wedge_is_closed.count(True) / wedge_reservoir_size
        kappa_t = 3 * rho_t

        # The use of 2 guarantees accurate adjustment for the double-counting inherent in the 
        # reservoir sampling method. Normalizes the value of T on calculation
        T_t = rho_t * (2 / (edge_reservoir_size * (edge_reservoir_size - 1))) * total_wedges

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


# Read streaming data
streaming_data = spark.readStream \
    .format("text") \
    .option("path", "data/fake.txt") \
    .load()

# Create datafrom with column "edge" containing the edge as a tuple of integers from value split by tab space
edges = streaming_data.withColumn("edge", split(col("value"), "\t").cast(ArrayType(IntegerType())))


# # Apply processing
# results = edges.withColumn(
#     "results",
#     expr("reservoir_sampling(monotonically_increasing_id(), edge)") ##TODO: change code to accept edge and parse them
# )
    # TODO: see line 136 comment
    # # Parse the edge
    # edge = tuple(map(int, edge_str.split("\t")))
    # if edge in edge_reservoir:
    #     return None  # Skip duplicate edges

# # Output results to console
# query = results.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()