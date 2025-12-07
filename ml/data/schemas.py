from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ShortType

schema_out = StructType([
    StructField("UserID", IntegerType(), True),
    StructField("Rating", IntegerType(), True),
    StructField("Date", DateType(), True),
    StructField("MovieID", IntegerType(), True),
])

user_schema = StructType([
        StructField("UserID", StringType(), True),
        StructField("Rating", IntegerType(), True),
        StructField("Date", DateType(), True)
    ])

movie_schema = StructType([
        StructField("MovieID", IntegerType(), True),
        StructField("YearOfRelease", ShortType(), True),
        StructField("Title", StringType(), True)
    ])