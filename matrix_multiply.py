from pyspark import SparkContext, SparkConf

# Initialize SparkContext
conf = SparkConf().setAppName("MatrixVectorMultiplication")
sc = SparkContext(conf=conf)

# Input matrix and vector
matrix = [
    (0, [1, 2, 3, 4]),
    (1, [5, 6, 7, 8]),
    (2, [9, 10, 11, 12])
]
vector = [2, 4, 6, 8]

# Broadcast the vector to all nodes in the cluster
broadcast_vector = sc.broadcast(vector)

# Perform matrix-vector multiplication using RDDs
result = sc.parallelize(matrix) \
    .map(lambda row: (row[0], sum(a * b for a, b in zip(row[1], broadcast_vector.value)))) \
    .collect()

# Print the result
for row_id, value in sorted(result):
    print(f"Row {row_id}: {value}")

# Stop SparkContext
sc.stop()
