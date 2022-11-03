# only print the data first
# insertion to database will be dealt with later

from pyspark.sql import SparkSession, functions as F, window as W


spark = SparkSession\
    .builder\
    .appName('GlobalAvgTemp')\
    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-19-openjdk/lib/postgresql-42.5.0.jar')\
    .getOrCreate()

weather_df = spark\
    .read\
    .option('header', 'true')\
    .option('inferSchema', 'true')\
    .csv('../cleaned_data/cleaned_weather_data.csv')

weather_df.show(10)

weather_df.printSchema()
