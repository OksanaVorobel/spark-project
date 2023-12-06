import pyspark.sql.functions as F
from pyspark.sql import Window

from dataframes import name_df, basics_df

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
