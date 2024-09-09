from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, DoubleType, StructType
from pyspark.sql.functions import col, current_timestamp, lit

# Create Spark session
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("constructorId", IntegerType(), True),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# Load the JSON file with the specified schema
df = spark.read.json(r"C:\Users\lenovo\Documents\GitHub\Pyspark\constructors.json", schema=schema)

# Show the DataFrame

df=df.withColumn("ingestion_date",current_timestamp())
df.show()