# NYC TLC TRIP PROJECT

# Getting Started
The goal of the project is to create a pipeline using NYC taxi data. Using this data to find the most popular pick-up points and destinations.
### DATA SUMMARY

**Yellow Taxi & Green Taxi:**
- Traditional taxi
- Some data is added manually by the driver.
- Passenger pick-up points for green taxis are limited.

**FHV & HVHFV :**
- They are vehicles that have 10,000 or more customers per day and have received the HVHFV license.
- Data is generated automatically through the application.

### PREREQUISITE
```
Airflow : 2.2.4
Python : 3.8
Pyspark : 3.2.1
Env: Local
```

### INSTALLING
**Data Source:** 
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

**Airflow:**

1- Airflow Installing on Local:
```
(base) fatmik@192 ~ % cd devel
(base) fatmik@192 devel % python3 -m venv env_airflow
(base) fatmik@192 devel % source env_airflow/bin/activate
(env_airflow) (base) fatmik@192 devel % pip install apache-airflow==2.2.4
```
I got error here. It was saying Building wheel for setproctitle (pyproject.toml) ... error error: subprocess-exited-with-error
it also said that it could not download xcode. It got resolved after downloading brew.

```
(base) fatmik@192 ~ %  brew install openssl
(base) fatmik@192 ~ %  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"\n
(base) fatmik@192 ~ %  echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> /Users/fatmik/.zprofile
(base) fatmik@192 ~ %  eval "$(/opt/homebrew/bin/brew shellenv)"
```
2- DB Setup and Creating a User

```
(env_airflow) (base) fatmik@192 devel % airflow db init
(env_airflow) (base) fatmik@192 airflow % ls
airflow.cfg     logs     airflow.db      webserver_config.py
(env_airflow) (base) fatmik@192 airflow % airflow users create \
> --username [**]\
> --password [**]\
> --firstname fatma \
> --lastname sahin \
> --role Admin \
> --email [mail]@icloud.com
```

3- WEBSERVER and SCHEDULER activate
```
(env_airflow) (base) fatmik@192 airflow % airflow webserver -D
(env_airflow) (base) fatmik@192 airflow % airflow scheduler -D
```

You can connect _localhost:8080_ with username and password! 


Change the spark default connection id. You can see the image below.

![Spark Connection Setting](https://github.com/fatmshn/NYC_Taxi_Project/blob/master/image/spark_connection.png)

### RUN
1- After airflow and its components are standing, dag should be able to work on the UI.
![Spark Connection Setting](https://github.com/fatmshn/NYC_Taxi_Project/blob/master/image/dag.png)
2- Run with spark submit on cli
```
#bin/bash
spark-submit \
  --master local \
  /Users/fatmik/PycharmProjects/NYC_Taxi_Project/DataPreparing.py \
  --date "2019-09"
```


### MAINTENANCE
On 05/13/2022, they are making the following changes to trip record files:
- The methods I read in csv file were arranged to read parquet file.
- They added a shared_match_flag column instead of the SR_Flag field in the fhvhv data, and the column content has also changed. Standardized to be able to union fhv and fhvhv dataframes.





