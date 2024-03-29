from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("SearchElement") \
    .getOrCreate()

# Define the data to be searched
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Create DataFrame from the data
df = spark.createDataFrame([(x,) for x in data], ["value"])

# Define the search element
search_element = 11  # Change the search element as needed

# Search for the element in the DataFrame
result_df = df.filter(col("value") == search_element)

# Check if the element is found
if result_df.count() > 0:
    print("Element found in the dataset")
else:
    print("Element not found in the dataset")

# Stop the Spark session
spark.stop()