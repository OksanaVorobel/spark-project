import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, when, lit, first, desc, array_contains, avg, dense_rank
from pyspark.sql import Window

from dataframes import name_df, basics_df, akas_df, ratings_df, episode_df

""" Oksana Vorobel queries """

def get_people_who_starred_in_genres_in_year():
    result = (
        name_df
        .join(basics_df, F.array_contains(name_df.knownForTitles, basics_df.tconst), "inner")
        .filter(
              (F.col("startYear").isNotNull())
              & (F.col("startYear") == 2023)
              & (F.array_contains(F.col("genres"), "Romance") | F.array_contains(F.col("genres"), "Drama"))
            )
        .select(name_df.columns)
        .distinct()
    )
    result.show()
    return result


def get_most_common_professions_by_years():
    window_spec = Window.partitionBy("primaryProfession").orderBy(F.desc("count"))
    result = (
        basics_df
            .filter((F.col("birthYear") >= 1980) & (F.col("birthYear") < 1990))
            .select("nconst", F.explode("primaryProfession").alias("primaryProfession"))
            .groupBy("primaryProfession").agg(F.count("nconst").alias("count"))
            .withColumn("rank", F.row_number().over(window_spec))
            .filter("rank = 1")
            .select("primaryProfession", "count")
    )
    result.show()
    return result


def get_most_common_birth():
    window_spec = Window.partitionBy("birthYear")
    result = (
        name_df
            .filter(F.col("birthYear").isNotNull())
            .withColumn("numPeopleBorn", F.count("nconst").over(window_spec))
            .orderBy(F.desc("numPeopleBorn"))
    )
    result.show()
    return result


def get_people_who_died_before_the_end_of_series():
    result = (
        name_df
        .join(basics_df, F.expr("array_contains(knownForTitles, tconst)"), "inner")
        .filter((F.col("titleType") == "tvSeries") & (F.col("deathYear").isNotNull()) & (F.col("deathYear") < F.col("endYear")))
        .orderBy(F.asc("deathYear"))
        .limit(50)
    )
    result.show()
    return result


def get_people_who_died_before_18():
    result = (
        name_df
        .filter((F.col("birthYear").isNotNull()) & (F.col("deathYear").isNotNull()) & (F.col("deathYear") - F.col("birthYear") < 18))
        .withColumn("deathAge", F.col("deathYear") - F.col("birthYear"))
        .orderBy(F.desc("deathYear"))
        .limit(25)
    )
    result.show()
    return result


def get_average_runtime_of_series_by_decade():
    result = (
        basics_df
        .filter((F.col("titleType") == "tvseries") & (F.col("startYear").isNotNull()))
        .withColumn("decade", F.expr("floor(startYear/10)*10"))
        .groupBy("decade")
        .agg(F.avg("runtimeMinutes").alias("averageRuntime"))
        .orderBy("decade")
    )
    result.show()
    return result


def get_top_5_most_credited_names():
    result = basics_df.select("primaryName", F.explode("knownForTitles").alias("knownForTitle")) \
        .groupBy("primaryName").agg(F.countDistinct("knownForTitle").alias("knownForCount")) \
        .orderBy(F.desc("knownForCount")).limit(5)

    result.show()
    return result


def run_name_df_queries():
    # 1. All people who starred in Romance or Drama in 2023
    result_q1 = get_people_who_starred_in_genres_in_year()
    (
        result_q1
            .withColumn("knownForTitles", F.col("knownForTitles").cast("string"))
            .withColumn("primaryProfession", F.col("primaryProfession").cast("string"))
            .write.mode("overwrite").csv('data/people_who_starred_in_Romance_or_Drama_2023.csv', header=True)
    )

    # 2. Most common primary professions among people born in the 1980s
    result_q2 = get_most_common_professions_by_years()
    result_q2.write.mode("overwrite").csv('data/most_common_professions_among_1980s_people.csv', header=True)

    # 3. Most common birth years among individuals
    result_q3 = get_most_common_birth()
    result_q3.write.mode("overwrite").csv('data/most_common_birth_years.csv', header=True)

    # 4. All people who died before the end of a TV series
    result_q4 = get_people_who_died_before_the_end_of_series()
    (
        result_q4
            .withColumn("knownForTitles", F.col("knownForTitles").cast("string"))
            .withColumn("primaryProfession", F.col("primaryProfession").cast("string"))
            .write.mode("overwrite").csv('data/people_who_died_before_the_end_of_series.csv', header=True)
     )
    # 5. All people who died before 18
    result_q5 = get_people_who_died_before_18()
    (
        result_q5
            .withColumn("knownForTitles", F.col("knownForTitles").cast("string"))
            .withColumn("primaryProfession", F.col("primaryProfession").cast("string"))
            .write.mode("overwrite").csv('data/people_who_died_before_18.csv', header=True)
    )
    # 6. The average runtime of TV Series by decade.
    result_q6 = get_average_runtime_of_series_by_decade()
    result_q6.write.mode("overwrite").csv('data/average_runtime_of_TV_series_by_decade.csv', header=True)

    # 7. top 5 most credited names along with their total count of knowns for the title
    result_q7 = get_top_5_most_credited_names()
    result_q7.write.mode("overwrite").csv('data/top_5_most_credited_names.csv', header=True)


""" end of Oksana Vorobel queries """


""" start of Ilona Klymenok queries """

def get_sum_of_film_translation():
    window = Window.partitionBy("titleId").orderBy(desc("isOriginalTitle"))
    result_q1 = (akas_df
                 .withColumn("original_title", first("title").over(window))
                 .groupBy("titleId", "original_title")
                 .agg(count("titleId").alias("translation_count"))
                 .orderBy(col("translation_count").desc()))
    return result_q1

def get_most_translated_languages():
    result_q2 = (akas_df
                 .filter((col("isOriginalTitle") == 0) & (col("language").isNotNull()))
                 .groupBy("language")
                 .agg(count("language").alias("translations_count"))
                 .orderBy(col("translations_count").desc()))
    return result_q2

def get_movies_by_type_n_region(type, region):
    result_q3 = akas_df.filter((array_contains("types", type)) & (col("region") == region))
    result_q3 = result_q3.select("titleId", "title", "types")
    return result_q3

def get_top_n_movies_by_highest_average_rating(n):
    filtered_ratings_df = ratings_df.filter(col("averageRating").isNotNull())
    result_q4 = (akas_df
                 .join(filtered_ratings_df, akas_df["titleId"] == filtered_ratings_df["tconst"], "inner")
                 .groupBy("region")
                 .agg(avg("averageRating").alias("average_rating"))
                 .orderBy(col("average_rating").desc())
                 .limit(n))
    return result_q4

def get_general_rating(type):
    # Визначення умов для розподілу рейтингів
    rating_condition = when((col("averageRating") >= 0) & (col("averageRating") < 4), "unsatisfactory") \
                      .when((col("averageRating") >= 4) & (col("averageRating") < 6), "satisfactory") \
                      .when((col("averageRating") >= 6) & (col("averageRating") < 8), "good") \
                      .when((col("averageRating") >= 8) & (col("averageRating") <= 10), "excellent")
    result_q5 = (akas_df
                 .join(ratings_df, akas_df["titleId"] == ratings_df["tconst"], "inner")
                 .withColumn("rating_category", rating_condition)
                 .filter((col("rating_category") == type) & (col("isOriginalTitle") == 1))
                 .select(akas_df["titleId"], "title", "rating_category", "averageRating")
                 .orderBy(col("averageRating")))
    return result_q5

def get_top_n_films_by_number_of_votes(n):
    # Визначення вікна для впорядкування за кількістю голосів
    window_spec = Window.orderBy(desc("numVotes"))
    result_q6 = (akas_df
                 .join(ratings_df, akas_df["titleId"] == ratings_df["tconst"], "inner")
                 .withColumn("rank", dense_rank().over(window_spec))
                 .filter((col("isOriginalTitle") == 1) & (col("rank") <= n))
                 .select(akas_df["titleId"], "title", "numVotes"))
    return result_q6

def run_akas_n_rating_queries():
    result_q1 = get_sum_of_film_translation()
    # result_q1.show(truncate=False)
    result_q1.coalesce(1).write.csv("data/sum_of_film_translation.csv", header=True, mode="overwrite")

    result_q2 = get_most_translated_languages()
    # result_q2.show(truncate=False)
    result_q2.coalesce(1).write.csv("data/most_translated_languages.csv", header=True, mode="overwrite")

    result_q3 = get_movies_by_type_n_region('tv', 'US')
    # result_q3.show(truncate=False)
    result_q3.coalesce(1).withColumn("types", col("types").cast("string")) \
                         .write.csv("data/movies_by_type_n_region.csv", header=True, mode="overwrite")

    result_q4 = get_top_n_movies_by_highest_average_rating(10)
    # result_q4.show(truncate=False)
    result_q4.coalesce(1).write.csv("data/top_10_movies_by_highest_average_rating.csv", header=True, mode="overwrite")

    result_q5 = get_general_rating('unsatisfactory')
    # result_q5.show(truncate=False)
    result_q5.coalesce(1).write.csv("data/movies_with_unsatisfactory_rating.csv", header=True, mode="overwrite")

    result_q6 = get_top_n_films_by_number_of_votes(10)
    # result_q6.show(truncate=False)
    result_q6.coalesce(1).write.csv("data/top_10_films_by_number_of_votes.csv", header=True, mode="overwrite")

""" end of Ilona Klymenok queries """




""" start of Синюк Олег queries """


def run_episodes_queries():
    # 1. all titles of TV series along with the total number of episodes, ordered by the total number of episodes in descending order
    result_df = (
        episode_df
            .join(basics_df, episode_df.parentTconst == basics_df.tconst)
            .filter(col('titleType') == 'tvSeries')
            .groupBy("primaryTitle")
            .agg(count(episode_df.tconst).alias('total_episodes'))
            .withColumnRenamed("count(tconst)", "total_episodes")
            .orderBy(col("total_episodes").desc())
    )
    result_df.show()
    result_df.write.mode("overwrite").csv('data/query1.csv', header=True)

    # 2. the season with the highest number of episodes for each TV series
    result_df = (
        episode_df
            .join(basics_df, episode_df.parentTconst == basics_df.tconst)
            .filter(col('titleType') == 'tvSeries')
            .groupBy("parentTconst", "seasonNumber")
            .agg(
            F.max("episodeNumber").alias("max_episode"),
            F.first(episode_df.tconst).alias("tconst")
        )
    )
    result_df.show()
    result_df.write.mode("overwrite").csv('data/query2.csv', header=True)

    # 3. TV series with the most episodes in each genre.
    result_df = (
        episode_df
            .join(basics_df, episode_df.parentTconst == basics_df.tconst)
            .filter(col('titleType') == 'tvSeries')
            .select(F.explode("genres").alias("genre"), "episodeNumber")
            .groupBy("genre")
            .agg(
            F.max("episodeNumber").alias("max_episodes")
        )
    )
    result_df.show()
    result_df.write.mode("overwrite").csv('data/query3.csv', header=True)

    # 4. List TV series that have at least one episode with a season number greater than 5.
    result_df = episode_df.filter("seasonNumber > 5").distinct()
    result_df.show()
    result_df.write.mode("overwrite").csv('data/query4.csv', header=True)

    # 5. List TV series along with the total number of seasons:
    result_df = episode_df.groupBy("parentTconst").agg(F.max("seasonNumber").alias("total_seasons"))
    result_df.show()
    result_df.write.mode("overwrite").csv('data/query5.csv', header=True)

    # 6. Identify TV series that have only one season:
    result_df = episode_df.groupBy("parentTconst").agg(F.max("seasonNumber").alias("total_seasons")).filter(
        "total_seasons = 1")
    result_df.show()
    result_df.write.mode("overwrite").csv('data/query6.csv', header=True)


""" end of Синюк Олег queries """
