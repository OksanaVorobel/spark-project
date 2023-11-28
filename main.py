from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

# Create a SparkSession
spark_session = (
    SparkSession.builder
        .master('local')
        .appName('test app')
        .config(conf=SparkConf())
        .getOrCreate()
)

# Create a test DataFrame
data = [(1, 'Oksana', 'Vorobel'), (2, 'Ilona', 'Klymenok')]
schema = t.StructType([
    t.StructField('id', t.ByteType(), True),
    t.StructField('name', t.StringType(), True),
    t.StructField('surname', t.StringType(), True)
])
df = spark_session.createDataFrame(data, schema)

# Display test DataFrame
df.show()

# Stop the SparkSession
spark_session.stop()

if __name__ == '__main__':
    print("It works!")
