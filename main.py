import pyspark.sql.types as t
from pyspark.sql import SparkSession

from queries import run_name_df_queries, run_akas_n_rating_queries
from session import spark_session, stop_session


def create_test_data(session: SparkSession):
    # Create a test DataFrame
    data = [(1, 'Oksana', 'Vorobel'), (2, 'Ilona', 'Klymenok')]
    schema = t.StructType([
        t.StructField('id', t.ByteType(), True),
        t.StructField('name', t.StringType(), True),
        t.StructField('surname', t.StringType(), True)
    ])
    df = session.createDataFrame(data, schema)

    # Display test DataFrame
    df.show()


if __name__ == '__main__':
    # create_test_data(spark_session)
    # run_name_df_queries()
    run_akas_n_rating_queries()
    stop_session()
