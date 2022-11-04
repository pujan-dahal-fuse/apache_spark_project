#modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

postgres_username = 'postgres'
postgres_password = ''

spark = SparkSession.builder.appName('Historical weather')\
    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-19-openjdk/lib/postgresql-42.5.0.jar')\
    .getOrCreate()

#weather dataset
weather_df = spark.read.option('inferSchema', 'true').option('header', 'true').csv('sample_data/cleaned_weather_data.csv')
weather_df.printSchema()
weather_df.show(5)
#continent_country dataset
country_cont_df= spark.read.option('inferSchema', 'true').option('header', 'true').csv('sample_data/cleaned_country_continent.csv')
country_cont_df.printSchema()
country_cont_df.show(5)

###Tasks


###1. Correlation between wind speed and average temperature
corr_df=weather_df.stat.corr('tavg','wspd')
        #wind speed and avg temp seems to have negative correlation.
        #Insight: Difference in temperature drives wind speed.
corr_df.show()

#inserting into db
corr_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.tavg_wspd_corr",
          properties={"user": postgres_username, "password": postgres_password})


###2. Standard deviation for average temperature for each country in the month of May each year, and find the country with lowest standard deviation.
avg_temp_may_df = weather_df\
                    .groupBy(year('date').alias('year'),month('date').alias('month'), 'country')\
                    .agg(min('tavg').alias('avg_temp'))\
                    .filter('month == 5')\
                    .orderBy('country', 'year')
avg_temp_may_df.show()
stddev_avg_temp_may_df = avg_temp_may_df\
                            .groupBy('country')\
                            .agg(stddev('avg_temp').alias('stddev'))\
                            .orderBy(asc('stddev'))\
                            .select('country',round('stddev', 1).alias('stddev_avg_temp_may'))
stddev_avg_temp_may_df.show()

stddev_avg_temp_may_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.stddev_avg_temp_may",
          properties={"user": postgres_username, "password": postgres_password})


###3. Max temperature that each country has ever had
max_temp_country_everyyeardf = weather_df\
                    .groupBy(year('date').alias('year'), 'country')\
                    .agg(max('tmax').alias('max_temp')).orderBy('country', 'year')
max_temp_country_everyyeardf.show()
max_temp_country_everhaddf = max_temp_country_everyyeardf\
                            .groupBy('country')\
                            .agg(max('max_temp').alias('maxtemp'))\
                            .orderBy('country')\
                            .select('country','maxtemp')
max_temp_country_everhaddf.show(10)                            
#inserting into db
max_temp_country_everhaddf\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.max_temp_country_everhad",
          properties={"user": postgres_username, "password": postgres_password})
###4. Top 5 hottest days in all Japan.
japan_weather_df=weather_df.orderBy(desc('tmax'))
japan_weather_df=japan_weather_df.filter(japan_weather_df.country=='Japan')
japan_weather_df.show(5)
#inserting into db
japan_weather_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.japan_weather",
          properties={"user": postgres_username, "password": postgres_password})


###5. Difference of each country’s average temperature from global average temperature for 2020. (Window Function)
avg_temp_df = weather_df\
                .filter(year('date') == 2020)\
                .groupBy('country')\
                .agg(mean('tavg').alias('avg_temp'))\
                .orderBy('country')
avg_temp_df.show()

global_avg_temp = (weather_df.select(mean('tavg')).collect()[0][0])
global_avg_temp

diff_global_avg_temp_df = avg_temp_df\
                            .withColumn('year', lit(2020))\
                            .withColumn('global_avg_temp', lit(global_avg_temp))\
                            .withColumn('difference', (col('avg_temp') - col('global_avg_temp')))

diff_global_avg_temp_df = diff_global_avg_temp_df.select('year', 'country', 'avg_temp', 'global_avg_temp', 'difference')
diff_global_avg_temp_df.show()
#inserting into db
diff_global_avg_temp_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.diff_global_avg_temp",
          properties={"user": postgres_username, "password": postgres_password})


###6. Rolling average of wind speed in Japan for the month of May 2021 (Window Function)
window_spec = Window.partitionBy('country').orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)

japan_may_2021_df = weather_df\
                        .filter(year('date') == 2021)\
                        .filter(month('date') ==5 )\
                        .filter('country == "Japan"')\
                        .withColumn('rolling_avg', mean('wspd').over(window_spec))\
                        .select('date', 'country', 'wspd', round('rolling_avg', 1).alias('rolling_avg'))
japan_may_2021_df.show()
#inserting into db
japan_may_2021_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.japan_may_2021",
          properties={"user": postgres_username, "password": postgres_password})


###7. Find hottest and coldest day of Japan for each month in 2021
window_spec1 = Window.partitionBy('country', year('date'), month('date')).orderBy('tmin')
window_spec2 = Window.partitionBy('country', year('date'), month('date')).orderBy(desc('tmax'))

japan_hot_cold_days_df=weather_df\
        .filter('country == "Japan"')\
        .filter(year('date') == 2021)\
        .withColumn('min_rank', dense_rank().over(window_spec1))\
        .withColumn('max_rank', dense_rank().over(window_spec2))\
        .withColumn('is_min_or_max', when(col('min_rank') == 1, lit('min')).when(col('max_rank') == 1, lit('max')))\
        .filter((col('min_rank') == 1) | (col('max_rank') == 1))\
        .select(to_date('date').alias('date'), 'country', 'tmin', 'tmax', 'is_min_or_max')
japan_hot_cold_days_df.show()
#inserting into db
japan_hot_cold_days_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.japan_hot_cold_days",
          properties={"user": postgres_username, "password": postgres_password})


###8. Find 3 windiest days of Canada for each month  in 2020
window_spec = Window.partitionBy('country', year('date'), month('date')).orderBy(desc('wspd'))

canda_2020_wspd_df=weather_df\
        .filter('country == "Canada"')\
        .filter(year('date') == 2020)\
        .withColumn('max_rank', dense_rank().over(window_spec))\
        .select(to_date('date').alias('date'), 'country', 'wspd')
canda_2020_wspd_df=canda_2020_wspd_df.orderBy(desc('wspd'))
canda_2020_wspd_df.show(3)
#inserting into db
canda_2020_wspd_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "weather.canda_2020_wspd",
          properties={"user": postgres_username, "password": postgres_password})


###9. Lowest wind speed for each continent (join) and the country, date on which it was recorded.
#joining two datasets
join_expr = weather_df['country'] == country_cont_df['country']
joined_df = weather_df.join(F.broadcast(country_cont_df), join_expr)

window_spec1 = Window.partitionBy('continent').orderBy(asc('wspd'))
window_spec2 = Window.partitionBy('continent').orderBy(desc('wspd'))

lowest_windspeed_continent_df = joined_df\
                            .withColumn('dense_rnk', F.dense_rank().over(window_spec1))\
                            .filter('dense_rnk == 1')\
                            .select('continent', weather_df['country'],'date', col('wspd').alias('lowestwindspeed')).distinct()

highest_windspeed_continent_df = joined_df\
                            .withColumn('dense_rnk', F.dense_rank().over(window_spec2))\
                            .filter('dense_rnk == 1')\
                            .select('continent', weather_df['country'],'date', col('wspd').alias('highestwindspeed')).distinct()
lowest_windspeed_continent_df.show()
highest_windspeed_continent_df.show()
#inserting into db
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