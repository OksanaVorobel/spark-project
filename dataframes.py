import os

from schemas import (
    basics_schema,
    name_schema,
    principals_schema,
    ratings_schema,
    episode_schema,
    crew_schema,
    akas_schema
)
from session import spark_session
from utils import create_dataframe, validate_title_basics_data, validate_akas_data, validate_crew_data, \
    validate_episode_data, validate_ratings_data, validate_principals_data, validate_name_data, \
    display_data_description, display_numerical_statistics


basics_df = validate_title_basics_data(create_dataframe(spark_session, basics_schema, os.path.join(os.getcwd(), "data/title.basics.tsv")))
akas_df = validate_akas_data(create_dataframe(spark_session, akas_schema, os.path.join(os.getcwd(), "data/title.akas.tsv")))
crew_df = validate_crew_data(create_dataframe(spark_session, crew_schema, os.path.join(os.getcwd(), "data/title.crew.tsv")))
episode_df = validate_episode_data(create_dataframe(spark_session, episode_schema, os.path.join(os.getcwd(), "data/title.episode.tsv")))
ratings_df = validate_ratings_data(create_dataframe(spark_session, ratings_schema, os.path.join(os.getcwd(), "data/title.ratings.tsv")))
principals_df = validate_principals_data(create_dataframe(spark_session, principals_schema, os.path.join(os.getcwd(), "data/title.principals.tsv")))
name_df = validate_name_data(create_dataframe(spark_session, name_schema, os.path.join(os.getcwd(), "data/name.basics.tsv")))

display_data_description(basics_df, "basics")
display_numerical_statistics(basics_df, ["startYear", "endYear", "runtimeMinutes"])

display_data_description(akas_df, "akas")
display_numerical_statistics(akas_df, ["ordering"])

display_data_description(crew_df, "crew")

display_data_description(episode_df, "episode")
display_numerical_statistics(episode_df, ["seasonNumber", "episodeNumber"])

display_data_description(ratings_df, "ratings")
display_numerical_statistics(ratings_df, ["averageRating", "numVotes"])

display_data_description(principals_df, "principals")
display_numerical_statistics(principals_df, ["ordering"])

display_data_description(name_df, "name")
display_numerical_statistics(name_df, ["birthYear", "deathYear"])
