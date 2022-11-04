from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, functions as fun, Window as Wd

postgres_username = 'memosha'
postgres_password = '1234'

# Creating a SparkSession in Python

spark = SparkSession.builder.appName('weather_history')    .config(
    'spark.driver.extraClassPath', '/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar')    .getOrCreate()

historical_weather_df = spark.read.option("header", True)      .csv(
    "../cleaned_data/cleaned_weather_data.csv")

historical_weather_df.show()

historical_weather_df.printSchema()

country_continent_df = spark.read.option("header", True)      .csv(
    "../cleaned_data/cleaned_country_continent.csv")

country_continent_df.show()

# 1. Correlation between latitude and average temperature


corr_df = historical_weather_df.select(fun.corr('Latitude', 'tavg'))

corr_df.show()

corr_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.corr_lat_tavg",
          properties={"user": postgres_username, "password": postgres_password})

#  2. Min temperature for each country in the month of May each year.


min_temp_may_df = historical_weather_df.groupBy(fun.year('date').alias('year'), fun.month('date').alias(
    'month'), 'country').agg(fun.min('tmin').alias('min_temp')).filter('month == 05').orderBy('country', 'year')

min_temp_may_df.show()

min_temp_may_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.min_temp_may",
          properties={"user": postgres_username, "password": postgres_password})

# 3. Standard deviation of min temperature for each country in the month of may each year.

stddev_min_temp_may_df = min_temp_may_df                            .groupBy('country')                            .agg(fun.stddev('min_temp').alias(
    'stddev'))                            .orderBy('country')                            .select('country', fun.round('stddev', 1).alias('stddev_min_temp_may'))

stddev_min_temp_may_df.show()

stddev_min_temp_may_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.stddev_min_temp_may",
          properties={"user": postgres_username, "password": postgres_password})


# 4. 5 windiest  days in Belgium

# use min avg temp , Window Function
window_spec = Wd.partitionBy('country').orderBy('wspd')
ranked_df = historical_weather_df                .withColumn('dense_rn', fun.dense_rank().over(
    window_spec))                .filter('dense_rn <= 5')                .filter('country == "Belgium"')

windiest_belgium_df = ranked_df.select('date', 'country', col(
    'wspd').alias('avg_wind'), col('dense_rn').alias('rank'))
windiest_belgium_df.show(5)

windiest_belgium_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.windiest_belgium",
          properties={"user": postgres_username, "password": postgres_password})


# 5. Difference of each country’s average temperature from global average temperature for 2021.  (Window Function)

avg_temp_df = historical_weather_df                .filter(fun.year('date') == 2021)                .groupBy(
    'country')                .agg(fun.mean('tavg').alias('avg_temp'))                .orderBy('country')

avg_temp_df.show()

global_avg_temp = round(historical_weather_df.select(
    fun.mean('tavg')).collect()[0][0], 1)
global_avg_temp  # 20.9

diff_global_avg_temp_df = avg_temp_df                            .withColumn('year', fun.lit(2021))                            .withColumn('avg_temp', fun.round(
    'avg_temp', 1))                            .withColumn('global_avg_temp', fun.lit(global_avg_temp))                            .withColumn('difference', fun.round(col('avg_temp') - col('global_avg_temp'), 1))

diff_global_avg_temp_df = diff_global_avg_temp_df.select(
    'year', 'country', 'avg_temp', 'global_avg_temp', 'difference')

diff_global_avg_temp_df.show()

avg_temp_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.avg_temp",
          properties={"user": postgres_username, "password": postgres_password})


# 6. Rolling average of temperature in Canada for the month of December 2020 (Window Function)

window_spec = Wd.partitionBy('country').orderBy(
    'date').rowsBetween(Wd.unboundedPreceding, Wd.currentRow)

canada_december_2020_df = historical_weather_df                        .filter(fun.year('date') == 2020)                        .filter(fun.month('date') == 12)                        .filter(
    'country == "Canada"')                        .withColumn('rolling_avg', fun.mean('tavg').over(window_spec))                        .select('date', 'country', 'tavg', fun.round('rolling_avg', 1).alias('rolling_avg'))

canada_december_2020_df.show()

canada_december_2020_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.canada_december_2020",
          properties={"user": postgres_username, "password": postgres_password})


# 7. Find hottest and coldest day of Belgium for each month in 2019.

window_spec1 = Wd.partitionBy('country', fun.year(
    'date'), fun.month('date')).orderBy('tmin')
window_spec2 = Wd.partitionBy('country', fun.year(
    'date'), fun.month('date')).orderBy(fun.desc('tmax'))

hottest_coldest_belgium_df = historical_weather_df        .filter('country == "Belgium"')        .filter(fun.year('date') == 2019)        .withColumn('min_rank', fun.dense_rank().over(window_spec1))        .withColumn('max_rank', fun.dense_rank().over(window_spec2))        .withColumn(
    'is_min_or_max', fun.when(col('min_rank') == 1, fun.lit('min')).when(col('max_rank') == 1, fun.lit('max')))        .filter((col('min_rank') == 1) | (col('max_rank') == 1))        .select(fun.to_date('date').alias('date'), 'country', 'tmin', 'tmax', 'is_min_or_max')

hottest_coldest_belgium_df.show()


hottest_coldest_belgium_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.hottest_coldest_belgium_df ",
          properties={"user": postgres_username, "password": postgres_password})


# 8. Find 4 windiest days of Canada for each month in 2021.

window_spec = Wd.partitionBy('country', fun.year(
    'date'), fun.month('date')).orderBy('wspd')

canada_2021_windiest_df = historical_weather_df        .filter('country == "Canada"')        .filter(fun.year('date') == 2021)        .withColumn(
    'max_rank', fun.dense_rank().over(window_spec))        .select(fun.to_date('date').alias('date'), 'country', 'wspd')
canada_2021_windiest_df.show(4)

canada_2021_windiest_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.canada_2021_windiest",
          properties={"user": postgres_username, "password": postgres_password})


# 9. Highest temperatures for each continent (join) and the country, date on which it was recorded.

join_expr = historical_weather_df['country'] == country_continent_df['country']

# since country dataset has a small size, we use broadcast join
joined_df = historical_weather_df.join(
    fun.broadcast(country_continent_df), join_expr)

window_spec = Wd.partitionBy('continent').orderBy('tmax')

highest_temp_continent_df = joined_df                            .withColumn('dense_rnk', fun.dense_rank().over(window_spec))                            .filter(
    'dense_rnk == 1')                            .select('continent', 'date', historical_weather_df['country'], col('tmax').alias('max_temp'))

highest_temp_continent_df.show()

highest_temp_continent_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.highest_temp_continent",
          properties={"user": postgres_username, "password": postgres_password})
