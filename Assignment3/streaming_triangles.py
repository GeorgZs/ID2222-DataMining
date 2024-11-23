import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, monotonically_increasing_id, col, udf
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, ArrayType
# Questions:
#   1. Do we wanna avoid duplicate edges?

# Avoid duplicates 
# Check the duplicates first ✓
# What if an edge arrives that happens to be the same edge of the wedge

# Parameters for reservoir sampling
EDGE_RESERVOIR_SIZE = 1000
WEDGE_RESERVOIR_SIZE = 1000

edge_reservoir = set()
wedge_reservoir = set()
wedge_is_closed = [False] * WEDGE_RESERVOIR_SIZE
total_wedges = 0
new_wedges = [] # TODO: set or not??
time = 0

# Initialize Spark session
spark = SparkSession.builder.appName("TriangleCountingReservoirSampling").getOrCreate()

def process_edge(edge_str):
    global edge_reservoir, wedge_reservoir, wedge_is_closed, total_wedges, new_wedges,time

    # Parse the edge
    edge = edge_str
    if edge in edge_reservoir:
        return None  # Skip duplicate edges

    for i, wedge in enumerate(wedge_reservoir):
        if is_closed_by(edge, wedge, wedge_reservoir):
            wedge_is_closed[i] = True

    # Edge reservoir sampling
    if len(edge_reservoir) < EDGE_RESERVOIR_SIZE:
        edge_reservoir.add(edge)

    else:
        if random.random() < 1 / time:
            edge_reservoir.add(edge) 
            total_wedges = update_total_wedges(edge_reservoir)
            new_wedges = generate_new_wedges(edge, edge_reservoir)

    # Wedge reservoir rampling
    for wedge in new_wedges:
        if len(wedge_reservoir) < WEDGE_RESERVOIR_SIZE:
            wedge_reservoir.add(wedge)

        else:
            if random.random() < len(new_wedges) / total_wedges:
                index = random.randint(0, WEDGE_RESERVOIR_SIZE - 1)
                wedge_reservoir.add(wedge)
                wedge_is_closed[index] = False
        
        # Check is_closed_by property for previous edges on newly added wedge?

        # Make sure to re-run new wedge reservoir on previous edges
        # if len(wedge_reservoir) == WEDGE_RESERVOIR_SIZE:
        #     for i, curr_edge in enumerate(edge_reservoir): # complexity ???? O(stream_size * new_wedges * edge_reservoir)
        #         if is_closed_by(curr_edge, wedge):
        #             wedge_is_closed[i] = True

    rho_t = wedge_is_closed.count(True) / WEDGE_RESERVOIR_SIZE
    kappa_t = 3 * rho_t

    # The use of 2 guarantees accurate adjustment for the double-counting inherent in the 
    # reservoir sampling method. Normalizes the value of T on calculation
    T_t = rho_t * (2 / (EDGE_RESERVOIR_SIZE * (EDGE_RESERVOIR_SIZE - 1))) * total_wedges

    # increase time after return 
    old_time = time
    time += 1
    return old_time, kappa_t, T_t
    
                


def is_closed_by(edge, wedge):
    # Check whether the two vertices of the edge are part of a wedge.
    # If they are then they form a triangle.
    return edge[0] in wedge and edge[1] in wedge

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
edges = spark.read.format("text").load("./data/web-BerkStan.txt")  # Replace with your file path

# Parse edges
edges = edges.withColumn("fromNode", split(col("value"), "\t")[0].cast("int")) \
             .withColumn("toNode", split(col("value"), "\t")[1].cast("int")) \
             .drop("value")  # Drop the original text column for clarity

# Define the return schema for process_edge (example: time, kappa_t, T_t)
result_schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("kappa_t", DoubleType(), True),
    StructField("T_t", DoubleType(), True)
])

# Register process_edge as a UDF and define result struct
@udf(result_schema)
def process_edge_udf(fromNode, toNode):
    edge = (fromNode, toNode)
    return process_edge(edge)  # Call the original process_edge function


# Apply processing on each row of the data frame
results = edges.withColumn(
    "results",
    process_edge_udf(col("fromNode"), col("toNode"))
)

# Output results to console (in batch mode)
results.show(truncate=False)