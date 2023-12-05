from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as func


def create_dataframe(spark_session: SparkSession, schema: t.StructType, file_name: str):
    return spark_session.read.option('delimiter', '\t').csv(file_name,
                                                            header=True,
                                                            nullValue='null',
                                                            dateFormat='MM/dd/yyyy',
                                                            schema=schema)


def validate_title_basics_data(df):
    df = df.replace('\\N', None)
    df = df.withColumn("genres", func.split(df["genres"], ",").cast("array<string>"))
    df = df.withColumn("isAdult", df["isAdult"].cast(t.BooleanType()))
    return df


def validate_akas_data(df):
    df = df.replace('\\N', None)
    df = df.withColumn("isOriginalTitle", df["isOriginalTitle"].cast(t.BooleanType()))
    df = df.withColumn("types", func.split(df["types"], ",").cast("array<string>"))
    df = df.withColumn("attributes", func.split(df["attributes"], ",").cast("array<string>"))
    return df


def validate_crew_data(df):
    df = df.replace('\\N', None)
    df = df.withColumn("directors", func.split(df["directors"], ",").cast("array<string>"))
    df = df.withColumn("writers", func.split(df["writers"], ",").cast("array<string>"))
    return df


def validate_episode_data(df):
    df = df.replace('\\N', None)
    return df


def validate_ratings_data(df):
    df = df.replace('\\N', None)
    return df


def validate_principals_data(df):
    df = df.replace('\\N', None)
    return df


def validate_name_data(df):
    df = df.replace('\\N', None)
    df = df.withColumn("primaryProfession", func.split(df["primaryProfession"], ",").cast("array<string>"))
    df = df.withColumn("knownForTitles", func.split(df["knownForTitles"], ",").cast("array<string>"))
    return df


def display_data_description(df, df_name: str):
    print('\n', df_name)
    df.show()  # Display the first few rows of the DataFrame
    print("Schema:")
    df.printSchema()  # Display the schema of the DataFrame
    print("Columns:")
    print(df.columns)

    print("Number of Columns:", len(df.columns))
    print("Number of Rows:", df.count())


def display_numerical_statistics(df, columns: list[str]):
    print("\nStatistics for Numerical Columns:")
    df.describe(columns).show()
