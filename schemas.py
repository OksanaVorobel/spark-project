import pyspark.sql.types as t

basics_schema = t.StructType([
    t.StructField("tconst", t.StringType(), False),
    t.StructField("titleType", t.StringType(), True),
    t.StructField("primaryTitle", t.StringType(), True),
    t.StructField("originalTitle", t.StringType(), True),
    t.StructField("isAdult", t.BooleanType(), True),
    t.StructField("startYear", t.IntegerType(), True),
    t.StructField("endYear", t.IntegerType(), True),
    t.StructField("runtimeMinutes", t.IntegerType(), True),
    t.StructField("genres", t.StringType(), True)
])


akas_schema = t.StructType([
    t.StructField("tconst", t.StringType(), False),
    t.StructField("ordering", t.IntegerType(), True),
    t.StructField("title", t.StringType(), True),
    t.StructField("region", t.StringType(), True),
    t.StructField("language", t.StringType(), True),
    t.StructField("types", t.StringType(), True),
    t.StructField("attributes", t.StringType(), True),
    t.StructField("isOriginalTitle", t.StringType(), True)
])


crew_schema = t.StructType([
    t.StructField("tconst", t.StringType(), False),
    t.StructField("directors", t.StringType(), True),
    t.StructField("writers", t.StringType(), True)
])

episode_schema = t.StructType([
    t.StructField("tconst", t.StringType(), False),
    t.StructField("parentTconst", t.StringType(), False),
    t.StructField("seasonNumber", t.IntegerType(), True),
    t.StructField("episodeNumber", t.IntegerType(), True)
])


ratings_schema = t.StructType([
    t.StructField("tconst", t.StringType(), False),
    t.StructField("averageRating", t.DoubleType(), False),
    t.StructField("numVotes", t.IntegerType(), True)
])


principals_schema = t.StructType([
    t.StructField("tconst", t.StringType(), False),
    t.StructField("ordering", t.IntegerType(), True),
    t.StructField("nconst", t.StringType(), False),
    t.StructField("category", t.StringType(), True),
    t.StructField("job", t.StringType(), True),
    t.StructField("characters", t.StringType(), True)
])


name_schema = t.StructType([
    t.StructField("tconst", t.StringType(), False),
    t.StructField("primaryName", t.StringType(), True),
    t.StructField("birthYear", t.IntegerType(), True),
    t.StructField("deathYear", t.IntegerType(), True),
    t.StructField("primaryProfession", t.StringType(), True),
    t.StructField("knownForTitles", t.StringType(), True)
])
