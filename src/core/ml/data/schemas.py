from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ShortType

user_schema = StructType([
        StructField("UserID", StringType(), True),
        StructField("Rating", IntegerType(), True),
        StructField("Date", DateType(), True)
    ])

movie_schema = StructType([
        StructField("MovieID", IntegerType(), False),
        StructField("YearOfRelease", ShortType(), False),
        StructField("Title", StringType(), False)
    ])
