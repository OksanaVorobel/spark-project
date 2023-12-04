from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

from schemas import (
    basics_schema,
    name_schema,
    principals_schema,
    ratings_schema,
    episode_schema,
    crew_schema,
    akas_schema
)
from utils import create_dataframe, validate_title_basics_data, validate_akas_data, validate_crew_data, \
    validate_episode_data, validate_ratings_data, validate_principals_data, validate_name_data, \
    display__data_description

# Create a SparkSession
spark_session = (
    SparkSession.builder
        .master('local')
        .appName('test app')
        .config(conf=SparkConf())
        .getOrCreate()
)


def create_test_data():
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

basics_df = validate_title_basics_data(create_dataframe(spark_session, basics_schema, "data/titlebasics.tsv"))
akas_df = validate_akas_data(create_dataframe(spark_session, akas_schema, "data/titleakas.tsv"))
crew_df = validate_crew_data(create_dataframe(spark_session, crew_schema, "data/titlecrew.tsv"))
episode_df = validate_episode_data(create_dataframe(spark_session, episode_schema, "data/titleepisode.tsv"))
ratings_df = validate_ratings_data(create_dataframe(spark_session, ratings_schema, "data/titleratings.tsv"))
principals_df = validate_principals_data(create_dataframe(spark_session, principals_schema, "data/titleprincipals.tsv"))
name_df = validate_name_data(create_dataframe(spark_session, name_schema, "data/namebasics.tsv"))

display__data_description(basics_df, "basics")
# display_numerical_statistics(basics_df, ["startYear", "endYear", "runtimeMinutes"])

display__data_description(akas_df, "akas")
# display_numerical_statistics(akas_df, ["ordering"])

display__data_description(crew_df, "crew")

display__data_description(episode_df, "episode")
# display_numerical_statistics(episode_df, ["seasonNumber", "episodeNumber"])

display__data_description(ratings_df, "ratings")
# display_numerical_statistics(ratings_df, ["averageRating", "numVotes"])

display__data_description(principals_df, "principals")
# display_numerical_statistics(principals_df, ["ordering"])

display__data_description(name_df, "name")
# display_numerical_statistics(name_df, ["birthYear", "deathYear"])

# if __name__ == '__main__':
#     create_test_data()
#     # Stop the SparkSession
spark_session.stop()
