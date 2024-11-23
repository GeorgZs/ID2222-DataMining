import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf, desc
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

class TriangleCounter:
    def __init__(self, edge_reservoir_size, wedge_reservoir_size):
        self.EDGE_RESERVOIR_SIZE = edge_reservoir_size
        self.WEDGE_RESERVOIR_SIZE = wedge_reservoir_size
        self.edge_reservoir = []
        self.wedge_reservoir = []
        self.wedge_is_closed = []
        self.total_wedges = 0
        self.time = 1

    def process_edge(self, edge):
        #print("Edge reservoir: ", self.edge_reservoir)
        #print("Wedge reservoir: ", self.wedge_reservoir)

        # Check if the new edge closes any wedges
        for i, wedge in enumerate(self.wedge_reservoir):
            if self.is_closed_by(edge, wedge):
                #print("Edge: ", edge, " closes wedge: ", wedge)
                self.wedge_is_closed[i] = True

        edge_reservoir_updated = False
        # Edge reservoir sampling
        if len(self.edge_reservoir) < self.EDGE_RESERVOIR_SIZE:
            if edge not in self.edge_reservoir:
                self.edge_reservoir.append(edge)
                edge_reservoir_updated = True

        elif random.random() <= 1 / self.time:
            random_index = random.randint(0, len(self.edge_reservoir) - 1)
            self.edge_reservoir[random_index] = edge
            edge_reservoir_updated = True

        if edge_reservoir_updated:
            self.total_wedges = self.update_total_wedges()
            new_wedges = self.generate_new_wedges(edge)
            #print("Total wedges: ", self.total_wedges)
            #print("New wedges: ", new_wedges)

            # Wedge reservoir sampling
            for new_wedge in new_wedges:
                if self.total_wedges:
                    if random.random() < len(new_wedges) / self.total_wedges:
                        if len(self.wedge_reservoir) < self.WEDGE_RESERVOIR_SIZE:
                            self.wedge_reservoir.append(new_wedge)
                            self.wedge_is_closed.append(False)

                        else:
                            random_index = random.randint(0, len(self.wedge_reservoir) - 1)
                            self.wedge_reservoir[random_index] = new_wedge
                            self.wedge_is_closed[random_index] = False

        # Calculate rho_t and kappa_t
        rho_t = sum(self.wedge_is_closed) / len(self.wedge_reservoir) if self.wedge_reservoir else 0
        kappa_t = 3 * rho_t

        # Calculate T_t (triangle count estimate)
        T_t = (rho_t * self.time * (self.time - 1) * self.total_wedges) / (2 * self.EDGE_RESERVOIR_SIZE * (self.EDGE_RESERVOIR_SIZE - 1)) if self.total_wedges > 0 else 0

        old_time = self.time
        self.time += 1
        
        #print(f"Time: {old_time}, Kappa_t: {kappa_t}, T_t: {T_t}")

        return old_time, kappa_t, T_t

    def is_closed_by(self, edge, wedge):
        return set(edge) <= set(wedge)

    def update_total_wedges(self):
        total = 0
        for i, (u, v) in enumerate(self.edge_reservoir):
            for j, (x, y) in enumerate(self.edge_reservoir[i+1:]):
                if u in (x, y) or v in (x, y):
                    total += 1
        return total

    def generate_new_wedges(self, edge):
        node_u, node_v = edge
        new_wedges = []
        for other_edge in self.edge_reservoir:

            if node_u in other_edge and node_v not in other_edge:
                other_node = other_edge[0] if other_edge[1] == node_u else other_edge[1]
                new_wedges.append((node_u, node_v, other_node))

            elif node_v in other_edge and node_u not in other_edge:
                other_node = other_edge[0] if other_edge[1] == node_v else other_edge[1]
                new_wedges.append((node_v, node_u, other_node))

        return new_wedges

    @staticmethod
    def find_neighbors(node, edge_reservoir):
        return [v for u, v in edge_reservoir if u == node] + [u for u, v in edge_reservoir if v == node]

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("TriangleCountingReservoirSampling").getOrCreate()

    # Create an instance of TriangleCounter
    triangle_counter = TriangleCounter(edge_reservoir_size=1000, wedge_reservoir_size=1000)

    # Define the return schema for process_edge
    result_schema = StructType([
        StructField("time", IntegerType(), True),
        StructField("kappa_t", DoubleType(), True),
        StructField("T_t", DoubleType(), True)
    ])

    # Register process_edge as a UDF
    @udf(result_schema)
    def process_edge_udf(fromNode, toNode):
        edge = (fromNode, toNode)
        return triangle_counter.process_edge(edge)

    # Read streaming data
    edges = spark.read.format("text").load("./data/web-BerkStan.txt")

    # Parse edges
    edges = edges.withColumn("fromNode", split(col("value"), "\t")[0].cast("int")) \
                .withColumn("toNode", split(col("value"), "\t")[1].cast("int")) \
                .drop("value")

    # Process all edges and cache the result
    results = edges.withColumn(
        "results",
        process_edge_udf(col("fromNode"), col("toNode"))
    ).cache()

    # Force evaluation of the entire dataset
    total_edges = results.count()

    print("First 20 processed edges:")
    results.show(20, truncate=False)

    print(f"20 edges from the middle (starting at row {total_edges // 2}):")
    results.orderBy("results.time").limit(20).offset(total_edges // 2).show(truncate=False)

    print("Last 20 processed edges:")
    results.orderBy(desc("results.time")).limit(20).show(truncate=False)


    spark.stop()


if __name__ == "__main__":
    main()
