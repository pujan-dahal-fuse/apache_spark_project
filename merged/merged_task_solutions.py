# load modules

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window

postgres_username = 'postgres'
postgres_password = ''

spark = SparkSession\
    .builder\
    .appName('MergedSolutions')\
    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-19-openjdk/lib/postgresql-42.5.0.jar')\
    .getOrCreate()


# load weather data
weather_df = spark\
    .read\
    .option('inferSchema', 'true')\
    .option('header', 'true')\
    .csv('MyFiles/zebra/fuse/spark_project/cleaned_data/cleaned_weather_data.csv')

weather_df.cache().show(10)
weather_df.printSchema()

# load continents data

continents_df = spark\
    .read\
    .option('inferSchema', 'true')\
    .option('header', 'true')\
    .csv('MyFiles/zebra/fuse/spark_project/cleaned_data/cleaned_country_continent.csv')

continents_df.cache().show(10)
continents_df.printSchema()


########
# PUJAN TASKS
########

#######
# Qn. 1. Find the global average temperature each year. Is it increasing ?
#######

global_avg_temp_df = weather_df\
    .groupBy(F.year('date').cast('integer').alias('year'))\
    .agg(F.mean('tavg').alias('tavg'))\
    .select('year', F.round('tavg', 1).alias('global_avg_temp'))\
    .orderBy('year')

global_avg_temp_df.show()


# insert records into database
global_avg_temp_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.global_avg_temp",
          properties={"user": postgres_username, "password": postgres_password})


##########
# Qn. 2. Correlation and population covariance between wind speed and air pressure.
##########
corr_covar_df = weather_df.select(
    F.corr('wspd', 'pres'), F.covar_pop('wspd', 'pres'))

corr_covar_df.show()

# insert into database
corr_covar_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.wspd_pressure_corr_covariance",
          properties={"user": postgres_username, "password": postgres_password})

########
# Qn. 3. Max temperature for each country in the month of July each year.
########
max_temp_july_df = weather_df\
    .groupBy(F.year('date').alias('year'), F.month('date').alias('month'), 'country')\
    .agg(F.max('tmax').alias('max_temp'))\
    .filter('month == 7')\
    .orderBy('country', 'year')

max_temp_july_df.show()
max_temp_july_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.max_temp_july",
          properties={"user": postgres_username, "password": postgres_password})

#########
# Qn. 4. Standard deviation of max temperature for each country in the month of july each year.
#########
# calculate the standard deviation for each country
stddev_max_temp_july_df = max_temp_july_df\
    .groupBy('country')\
    .agg(F.stddev('max_temp').alias('stddev'))\
    .orderBy('country')\
    .select('country', F.round('stddev', 1).alias('stddev_max_temp_july'))

stddev_max_temp_july_df.show()

# insert into database
stddev_max_temp_july_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.stddev_max_temp_july",
          properties={"user": postgres_username, "password": postgres_password})

#########
# Qn. 5. Min temperature that each country has ever had and the date at which it happened (window function)
########
window_spec = Window.partitionBy('country').orderBy('tmin')

ranked_df = weather_df.withColumn(
    'dense_rn', F.dense_rank().over(window_spec)).filter('dense_rn == 1')

min_temp_df = ranked_df.select(
    'date', 'country', col('tmin').alias('min_temp'))

min_temp_df.show()

# insert into database
min_temp_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.min_temp_each_country",
          properties={"user": postgres_username, "password": postgres_password})

########
# Qn. 6. Find the 5 coldest days in Canada (use min avg temp) (Window Function)
#######

window_spec = Window.partitionBy('country').orderBy('tavg')
ranked_df = weather_df\
    .withColumn('dense_rn', F.dense_rank().over(window_spec))\
    .filter('dense_rn <= 5')\
    .filter('country == "Canada"')

coldest_days_in_Canada_df = ranked_df.select('date', 'country', col(
    'tavg').alias('avg_temperature'), col('dense_rn').alias('rank'))
coldest_days_in_Canada_df.show()

# insert into database
coldest_days_in_Canada_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.coldest_days_in_Canada",
          properties={"user": postgres_username, "password": postgres_password})

#########
# Qn. 7. Difference of each country’s average temperature from global average temperature for 2019.
#########


avg_temp_df = weather_df\
    .filter(F.year('date') == 2019)\
    .groupBy('country')\
    .agg(F.mean('tavg').alias('avg_temp'))\
    .orderBy('country')

global_avg_temp = round(weather_df.select(F.mean('tavg')).collect()[0][0], 1)
global_avg_temp  # 20.9

diff_global_avg_temp_df = avg_temp_df\
    .withColumn('year', F.lit(2019))\
    .withColumn('avg_temp', F.round('avg_temp', 1))\
    .withColumn('global_avg_temp', F.lit(global_avg_temp))\
    .withColumn('difference', F.round(col('avg_temp') - col('global_avg_temp'), 1))

diff_global_avg_temp_df = diff_global_avg_temp_df.select(
    'year', 'country', 'avg_temp', 'global_avg_temp', 'difference')

diff_global_avg_temp_df.show()


# insert into database
diff_global_avg_temp_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.diff_global_avg_temp",
          properties={"user": postgres_username, "password": postgres_password})

#######
# Qn. 8. Rolling average of temperature in Belgium for the month of July 2019 (Window Function)
########

window_spec = Window.partitionBy('country').orderBy(
    'date').rowsBetween(Window.unboundedPreceding, Window.currentRow)

belgium_july_2019_df = weather_df\
    .filter(F.year('date') == 2019)\
    .filter(F.month('date') == 7)\
    .filter('country == "Belgium"')\
    .withColumn('rolling_avg', F.mean('tavg').over(window_spec))\
    .select('date', 'country', 'tavg', F.round('rolling_avg', 1).alias('rolling_avg'))

belgium_july_2019_df.show()


# insert into database
belgium_july_2019_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.belgium_july_2019_temp_rolling_avg",
          properties={"user": postgres_username, "password": postgres_password})


#######
# Qn. 9. Find hottest and coldest day of UK for each month in 2020 (use tmin, tmax)
#######

window_spec1 = Window.partitionBy('country', F.year(
    'date'), F.month('date')).orderBy('tmin')
window_spec2 = Window.partitionBy('country', F.year(
    'date'), F.month('date')).orderBy(F.desc('tmax'))

hottest_coldest_day_UK_df = weather_df\
    .filter('country == "United Kingdom"')\
    .filter(F.year('date') == 2020)\
    .withColumn('min_rank', F.dense_rank().over(window_spec1))\
    .withColumn('max_rank', F.dense_rank().over(window_spec2))\
    .withColumn('is_min_or_max', F.when(col('min_rank') == 1, F.lit('min')).when(col('max_rank') == 1, F.lit('max')))\
    .filter((col('min_rank') == 1) | (col('max_rank') == 1))\
    .select('date', 'country', 'tmin', 'tmax', 'is_min_or_max')

hottest_coldest_day_UK_df.show()


# insert into database
hottest_coldest_day_UK_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.hottest_coldes_day_UK",
          properties={"user": postgres_username, "password": postgres_password})

#######
# Qn. 10. Fastest wind speed for USA and Canada each month of 2021 (pivot table)
#######
pivoted_df = weather_df.groupBy(
    F.year('date'), F.month('date')).pivot('Country').max()
fastest_wspd_df = pivoted_df\
    .where(col('year(date)') == 2021)\
    .select(col('year(date)').alias('year'), col('month(date)').alias('month'), 'United States_max(wspd)', 'Canada_max(wspd)')\
    .orderBy('month')

fastest_wspd_df.show()


# insert into database
fastest_wspd_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.fastest_wspd_USA_Canada",
          properties={"user": postgres_username, "password": postgres_password})

#######
# Qn. 11. Lowest temperatures for each continent (join) and the country, date on which it was recorded.
#######
join_expr = weather_df['country'] == continents_df['country']

# since country dataset has a small size, we use broadcast join
joined_df = weather_df.join(F.broadcast(continents_df), join_expr)

window_spec = Window.partitionBy('continent').orderBy('tmin')

lowest_temp_continent_df = joined_df\
    .withColumn('dense_rnk', F.dense_rank().over(window_spec))\
    .filter('dense_rnk == 1')\
    .select('continent', 'date', weather_df['country'], col('tmin').alias('min_temp'))

lowest_temp_continent_df.show()


# insert into database
lowest_temp_continent_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.lowest_temp_continent",
          properties={"user": postgres_username, "password": postgres_password})


##########
# MEMOSHA TASKS
##########

# 1. Correlation between latitude and average temperature


corr_df = weather_df.select(F.corr('Latitude', 'tavg'))

corr_df.show()

corr_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.corr_lat_tavg",
          properties={"user": postgres_username, "password": postgres_password})

#  2. Min temperature for each country in the month of May each year.


min_temp_may_df = weather_df\
    .groupBy(F.year('date').alias('year'), F.month('date').alias('month'), 'country')\
    .agg(F.min('tmin').alias('min_temp'))\
    .filter('month == 05')\
    .orderBy('country', 'year')

min_temp_may_df.show()

min_temp_may_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.min_temp_may",
          properties={"user": postgres_username, "password": postgres_password})

# 3. Standard deviation of min temperature for each country in the month of may each year.

stddev_min_temp_may_df = min_temp_may_df\
    .groupBy('country')\
    .agg(F.stddev('min_temp').alias('stddev'))\
    .orderBy('country')\
    .select('country', F.round('stddev', 1).alias('stddev_min_temp_may'))

stddev_min_temp_may_df.show()

stddev_min_temp_may_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.stddev_min_temp_may",
          properties={"user": postgres_username, "password": postgres_password})


# 4. 5 windiest  days in Belgium

# use min avg temp , Window Function
window_spec = Window.partitionBy('country').orderBy('wspd')

ranked_df = weather_df\
    .withColumn('dense_rn', F.dense_rank().over(window_spec))\
    .filter('dense_rn <= 5')\
    .filter('country == "Belgium"')

windiest_belgium_df = ranked_df\
    .select('date', 'country', col('wspd').alias('avg_wind'), col('dense_rn').alias('rank'))

windiest_belgium_df.show(5)

windiest_belgium_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.windiest_belgium",
          properties={"user": postgres_username, "password": postgres_password})


# 5. Difference of each country’s average temperature from global average temperature for 2021.  (Window Function)

avg_temp_df = weather_df\
    .filter(F.year('date') == 2021)\
    .groupBy('country')\
    .agg(F.mean('tavg').alias('avg_temp'))\
    .orderBy('country')

avg_temp_df.show()

global_avg_temp = round(weather_df.select(F.mean('tavg')).collect()[0][0], 1)
global_avg_temp  # 20.9

diff_global_avg_temp_df = avg_temp_df\
    .withColumn('year', F.lit(2021))\
    .withColumn('avg_temp', F.round('avg_temp', 1))\
    .withColumn('global_avg_temp', F.lit(global_avg_temp))\
    .withColumn('difference', F.round(col('avg_temp') - col('global_avg_temp'), 1))

diff_global_avg_temp_df = diff_global_avg_temp_df\
    .select('year', 'country', 'avg_temp', 'global_avg_temp', 'difference')

diff_global_avg_temp_df.show()

avg_temp_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.avg_temp",
          properties={"user": postgres_username, "password": postgres_password})


# 6. Rolling average of temperature in Canada for the month of December 2020 (Window Function)

window_spec = Window.partitionBy('country')\
    .orderBy('date')\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

canada_december_2020_df = weather_df\
    .filter(F.year('date') == 2020)\
    .filter(F.month('date') == 12)\
    .filter('country == "Canada"')\
    .withColumn('rolling_avg', F.mean('tavg').over(window_spec))\
    .select('date', 'country', 'tavg', F.round('rolling_avg', 1).alias('rolling_avg'))

canada_december_2020_df.show()

canada_december_2020_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.canada_december_2020",
          properties={"user": postgres_username, "password": postgres_password})


# 7. Find hottest and coldest day of Belgium for each month in 2019.

window_spec1 = Window.partitionBy('country', F.year(
    'date'), F.month('date')).orderBy('tmin')
window_spec2 = Window.partitionBy('country', F.year(
    'date'), F.month('date')).orderBy(F.desc('tmax'))

hottest_coldest_belgium_df = weather_df\
    .filter('country == "Belgium"')\
    .filter(F.year('date') == 2019)\
    .withColumn('min_rank', F.dense_rank().over(window_spec1))\
    .withColumn('max_rank', F.dense_rank().over(window_spec2))\
    .withColumn('is_min_or_max', F.when(col('min_rank') == 1, F.lit('min')).when(col('max_rank') == 1, F.lit('max')))\
    .filter((col('min_rank') == 1) | (col('max_rank') == 1))\
    .select(F.to_date('date').alias('date'), 'country', 'tmin', 'tmax', 'is_min_or_max')

hottest_coldest_belgium_df.show()


hottest_coldest_belgium_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.hottest_coldest_belgium_df ",
          properties={"user": postgres_username, "password": postgres_password})


# 8. Find 4 windiest days of Canada for each month in 2021.

window_spec = Window.partitionBy('country', F.year(
    'date'), F.month('date')).orderBy('wspd')

canada_2021_windiest_df = weather_df\
    .filter('country == "Canada"')\
    .filter(F.year('date') == 2021)\
    .withColumn('max_rank', F.dense_rank().over(window_spec))\
    .select(F.to_date('date').alias('date'), 'country', 'wspd')

canada_2021_windiest_df.show(4)

canada_2021_windiest_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.canada_2021_windiest",
          properties={"user": postgres_username, "password": postgres_password})


# 9. Highest temperatures for each continent (join) and the country, date on which it was recorded.

join_expr = weather_df['country'] == continents_df['country']

# since country dataset has a small size, we use broadcast join
joined_df = weather_df.join(
    F.broadcast(continents_df), join_expr)

window_spec = Window.partitionBy('continent').orderBy('tmax')

highest_temp_continent_df = joined_df\
    .withColumn('dense_rnk', F.dense_rank().over(window_spec))\
    .filter('dense_rnk == 1')\
    .select('continent', 'date', weather_df['country'], col('tmax').alias('max_temp'))

highest_temp_continent_df.show()

highest_temp_continent_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.highest_temp_continent",
          properties={"user": postgres_username, "password": postgres_password})


##########
# Akshey Tasks
##########

# 1. Correlation between wind speed and average temperature
corr_df = weather_df.select(F.corr('tavg', 'wspd'))
# wind speed and avg temp seems to have negative correlation.
# Insight: Difference in temperature drives wind speed.
corr_df.show()

# inserting into db
corr_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.tavg_wspd_corr",
          properties={"user": postgres_username, "password": postgres_password})


# 2. Standard deviation for average temperature for each country in the month of May each year, and find the country with lowest standard deviation.
avg_temp_may_df = weather_df\
    .groupBy(F.year('date').alias('year'), F.month('date').alias('month'), 'country')\
    .agg(F.min('tavg').alias('avg_temp'))\
    .filter('month == 5')\
    .orderBy('country', 'year')

avg_temp_may_df.show()

stddev_avg_temp_may_df = avg_temp_may_df\
    .groupBy('country')\
    .agg(F.stddev('avg_temp').alias('stddev'))\
    .orderBy(F.asc('stddev'))\
    .select('country', F.round('stddev', 1).alias('stddev_avg_temp_may'))

stddev_avg_temp_may_df.show()

stddev_avg_temp_may_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.stddev_avg_temp_may",
          properties={"user": postgres_username, "password": postgres_password})


# 3. Max temperature that each country has ever had
max_temp_country_everyyeardf = weather_df\
    .groupBy(F.year('date').alias('year'), 'country')\
    .agg(F.max('tmax').alias('max_temp')).orderBy('country', 'year')

max_temp_country_everyyeardf.show()

max_temp_country_everhaddf = max_temp_country_everyyeardf\
    .groupBy('country')\
    .agg(F.max('max_temp').alias('maxtemp'))\
    .orderBy('country')\
    .select('country', 'maxtemp')

max_temp_country_everhaddf.show(10)

# inserting into db
max_temp_country_everhaddf\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.max_temp_country_everhad",
          properties={"user": postgres_username, "password": postgres_password})


# 4. Top 5 hottest days in all Japan.
japan_weather_df = weather_df.orderBy(F.desc('tmax'))
japan_weather_df = japan_weather_df.filter(japan_weather_df.country == 'Japan')
japan_weather_df.show(5)

# inserting into db
japan_weather_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.japan_weather",
          properties={"user": postgres_username, "password": postgres_password})


# 5. Difference of each country’s average temperature from global average temperature for 2020. (Window Function)
avg_temp_df = weather_df\
    .filter(F.year('date') == 2020)\
    .groupBy('country')\
    .agg(F.mean('tavg').alias('avg_temp'))\
    .orderBy('country')

avg_temp_df.show()

global_avg_temp = (weather_df.select(F.mean('tavg')).collect()[0][0])
global_avg_temp

diff_global_avg_temp_df = avg_temp_df\
    .withColumn('year', F.lit(2020))\
    .withColumn('global_avg_temp', F.lit(global_avg_temp))\
    .withColumn('difference', (col('avg_temp') - col('global_avg_temp')))

diff_global_avg_temp_df = diff_global_avg_temp_df.select(
    'year', 'country', 'avg_temp', 'global_avg_temp', 'difference')
diff_global_avg_temp_df.show()
# inserting into db
diff_global_avg_temp_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.diff_global_avg_temp",
          properties={"user": postgres_username, "password": postgres_password})


# 6. Rolling average of wind speed in Japan for the month of May 2021 (Window Function)
window_spec = Window.partitionBy('country').orderBy(
    'date').rowsBetween(Window.unboundedPreceding, Window.currentRow)

japan_may_2021_df = weather_df\
    .filter(F.year('date') == 2021)\
    .filter(F.month('date') == 5)\
    .filter('country == "Japan"')\
    .withColumn('rolling_avg', F.mean('wspd').over(window_spec))\
    .select('date', 'country', 'wspd', F.round('rolling_avg', 1).alias('rolling_avg'))
japan_may_2021_df.show()
# inserting into db
japan_may_2021_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.japan_may_2021",
          properties={"user": postgres_username, "password": postgres_password})


# 7. Find hottest and coldest day of Japan for each month in 2021
window_spec1 = Window.partitionBy(
    'country', F.year('date'), F.month('date')).orderBy('tmin')
window_spec2 = Window.partitionBy('country', F.year(
    'date'), F.month('date')).orderBy(F.desc('tmax'))

japan_hot_cold_days_df = weather_df\
    .filter('country == "Japan"')\
    .filter(F.year('date') == 2021)\
    .withColumn('min_rank', F.dense_rank().over(window_spec1))\
    .withColumn('max_rank', F.dense_rank().over(window_spec2))\
    .withColumn('is_min_or_max', F.when(col('min_rank') == 1, F.lit('min')).when(col('max_rank') == 1, F.lit('max')))\
    .filter((col('min_rank') == 1) | (col('max_rank') == 1))\
    .select(F.to_date('date').alias('date'), 'country', 'tmin', 'tmax', 'is_min_or_max')
japan_hot_cold_days_df.show()
# inserting into db
japan_hot_cold_days_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.japan_hot_cold_days",
          properties={"user": postgres_username, "password": postgres_password})


# 8. Find 3 windiest days of Canada for each month  in 2020
window_spec = Window.partitionBy('country', F.year(
    'date'), F.month('date')).orderBy(F.desc('wspd'))

canda_2020_wspd_df = weather_df\
    .filter('country == "Canada"')\
    .filter(F.year('date') == 2020)\
    .withColumn('max_rank', F.dense_rank().over(window_spec))\
    .select(F.to_date('date').alias('date'), 'country', 'wspd')
canda_2020_wspd_df = canda_2020_wspd_df.orderBy(F.desc('wspd'))
canda_2020_wspd_df.show(3)
# inserting into db
canda_2020_wspd_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.canda_2020_wspd",
          properties={"user": postgres_username, "password": postgres_password})


# 9. Lowest wind speed for each continent (join) and the country, date on which it was recorded.
# joining two datasets
join_expr = weather_df['country'] == continents_df['country']
joined_df = weather_df.join(F.broadcast(continents_df), join_expr)

window_spec1 = Window.partitionBy('continent').orderBy(F.asc('wspd'))
window_spec2 = Window.partitionBy('continent').orderBy(F.desc('wspd'))

lowest_windspeed_continent_df = joined_df\
    .withColumn('dense_rnk', F.dense_rank().over(window_spec1))\
    .filter('dense_rnk == 1')\
    .select('continent', weather_df['country'], 'date', col('wspd').alias('lowestwindspeed')).distinct()

highest_windspeed_continent_df = joined_df\
    .withColumn('dense_rnk', F.dense_rank().over(window_spec2))\
    .filter('dense_rnk == 1')\
    .select('continent', weather_df['country'], 'date', col('wspd').alias('highestwindspeed')).distinct()
lowest_windspeed_continent_df.show()
highest_windspeed_continent_df.show()


# inserting into db
lowest_windspeed_continent_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.lowest_windspeed_continent_df",
          properties={"user": postgres_username, "password": postgres_password})
highest_windspeed_continent_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.highest_windspeed_continent_df",
          properties={"user": postgres_username, "password": postgres_password})
