# **Apache Spark Project**

In this project, analysis on [Historical Weather Data](https://www.kaggle.com/datasets/balabaskar/historical-weather-data-of-all-country-capitals) was performed using Apache Spark. This was a group project that was performed as a part of Traineeship program at Fusemachines Nepal, and the collaborators are [Akshey Sigdel](https://www.github.com/aksigdel), [Memosha Joshi](https://www.github.com/MemoshaJoshi) and [Pujan Dahal](https://www.github.com/pujan-dahal). The pipeline diagram for the project is shown below:
![pipeline_diagram](spark_project_pipeline_diagram.png)

## **Project Pipeline**

The project took its datasets i.e. [Historical Weather Data](https://www.kaggle.com/datasets/balabaskar/historical-weather-data-of-all-country-capitals) and[Country continent data](https://www.kaggle.com/datasets/statchaitya/country-to-continent) from [kaggle](https://www.kaggle.com). The data was downloaded as a .csv file, which was loaded to Apache Spark running in local mode. Spark was used to perform preprocessing and transformation operations on the data. After solving few queries using transformation operations, the output data was exported to PostgreSQL database in the form of tables. PostgreSQL is the destination in this pipeline.

## **Contents**

Initial data ingested into Spark were stored in [`data`](data/) folder. Preprocessing of data was done in [`preprocessing.ipynb`](preprocessing.ipynb) file using Spark, and cleaned data was stored in [`cleaned_data`](cleaned_data/) folder. After preprocessing, each collaborator performed their individual transformations and exported respective tables to their databases in their respective named folders i.e. [akshey](akshey/), [memosha](memosha/) and [pujan](pujan/). Finally merged codes were stored in [merged](merged/) folder.

Each individuals' folder consists of a .ipynb and a .py file. The .ipynb files were used to visualize the output results, as it is easier in a notebook file format. Running .py files in each individual's folder exports transformed dataframes to PostgreSQL in the form of tables.

## **Running the Project**

To run the project, you need to have linux installed and python3, python3-pip installed in it. After cloning this repository, run the following command to install all required pip packages:

```
pip install -r requirements.txt
```

You can run all the .ipynb files after this. In order to run the .py files, you need to first install PostgreSQL, create a database named `spark` and a table named `weather`. Follow these steps in order to perform spark-submit command:

1. Download the platform independent .tar.gz file from the following link: [https://jdbc.postgresql.org/download/](https://jdbc.postgresql.org/download/)
2. Extract the file.
3. Copy the extracted .jar file to `/usr/lib/jvm/[java_folder]/lib/` folder.
4. Change your database login credentials in [`merged/merged_task_solutions.py`](merged/merged_task_solutions.py) and other relevent .py files.
5. Run the following command while doing spark-submit for .py files:

```
spark-submit --driver-class-path <path_to_jar.jar> <python_file.py>
```

## **Individual Tasks**

Tasks performed by each individual are listed below:

### [**Akshey Sigdel**](https://www.github.com/aksigdel)

1. Correlation between wind speed and average temperature
2. Standard deviation for average temperature for each country in the month of May each year, and find the country with lowest standard deviation.
3. Max temperature that each country has ever had
4. Top 5 hottest days in all Japan.
5. Difference of each country’s average temperature from global average temperature for 2020. (Window Function)
6. Rolling average of wind speed in Japan for the month of May 2021 (Window Function)
7. Find hottest and coldest day of Japan for each month in 2021
8. Find 3 windiest days of Canada for each month in 2020
9. Highest and Lowest wind speed for each continent (join) and the country, date on which it was recorded.

### [**Memosha Joshi**](https://www.github.com/MemoshaJoshi)

1. Correlation between latitude and average temperature
2. Min temperature for each country in the month of May each year.
3. Standard deviation of min temperature for each country in the month of may each year.
4. Find the 5 windiest days in Belgium
5. Difference of each country’s average temperature from global average temperature for 2021. (Window Function)
6. Rolling average of temperature in Canada for the month of December 2020 (Window Function)
7. Find hottest and coldest day of Australia for each month in 2019.
8. Find 4 windiest days of Canada in 2020.
9. Highest temperatures for each continent (join) and the country, date on which it was recorded.

### [**Pujan Dahal**](https://www.github.com/pujan-dahal)

1. Find the global average temperature each year. Is it increasing ?
2. Correlation and population covariance between wind speed and air pressure.
3. Max temperature for each country in the month of July each year.
4. Standard deviation of max temperature for each country in the month of july each year.
5. Min temperature that each country has ever had and the date at which it happened (window function)
6. Find the 5 coldest days in Canada (use min avg temp) (Window Function)
7. Difference of each country’s average temperature from global average temperature for 2019.
8. Rolling average of temperature in Belgium for the month of July 2019 (Window Function)
9. Find hottest and coldest day of UK for each month in 2020 (use tmin, tmax)
10. Fastest wind speed for USA and Canada each month of 2021 (pivot table)
11. Lowest temperatures for each continent (join) and the country, date on which it was recorded.
