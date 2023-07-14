# spark
import json
import requests
import sys
from pyspark.sql.functions import * 
import datetime as dt
from pyspark.sql.functions import col,current_timestamp
#Creating DataFrames for all files in source.
sample = dbutils.fs.ls("/tmp/source_db")
#creating a dataframe for files list
df = spark.createDataFrame(sample)
#display(df)

#filtering out circuits[1]
filtering_df = df.filter("name != 'circuits[1].csv'")

file_list = [row[0] for row in filtering_df.select("name").collect()]
# print(file_list)

#UDF for creating DataFrames in for-loop
def read_csv_file(file_list_name):
    df1 = spark.read.format("csv").option("header", True)\
                    .option("inferschema",True)\
                    .load("/tmp/source-db/" + file_list_name)
    return df1


for file_list_name in file_list:
    df_name ="df_"+ file_list_name.replace(".csv", "")
    globals()[df_name] = read_csv_file(file_list_name)

#display(drivers_df)
df1_circuits=df_circuits.withColumnRenamed("circuitId","circuit_Id")\
                    .withColumnRenamed("circuitRef","circuit_Ref")\
                    .withColumnRenamed("lat","latitude")\
                    .withColumnRenamed("lng","lngitude")\
                    .withColumnRenamed("alt","altitude")\
                    .drop("url")\
                    .withColumn("ingestion_date", current_timestamp())
#display(df1_circuits)
df1_constructor_results=df_constructor_results.withColumnRenamed("constructorResultsId","constructorResults_Id")\
                       .withColumnRenamed("raceId","race_Id")\
                       .withColumnRenamed("constructorId","constructor_Id")
#display(df1_constructor_results)
df_constructor_standings=spark.read.format("csv")\
                                     .option("header",True)\
                                     .option("inferschema",True)\
                                     .load("dbfs:/tmp/source_db/constructors.csv")\
                                     .withColumnRenamed("constructorId","constructor_Id")\
                                     .withColumnRenamed("constructorRef","constructor_Ref")\
                                     .withColumnRenamed("name","Name")\
                                     .drop("url")
#display(constructors)

df_constructors.withColumnRenamed("constructorId","constructor_Id")\
               .withColumnRenamed("constructorRef","constructor_Ref")\
               .withColumnRenamed("name","constructor_Name")\
               .drop("url")

# display(df_constructors)
df_sprint_results=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/driver_standings.csv")\
    .withColumnRenamed("sprintResultId","sprintResult_Id")\
    .withColumnRenamed("raceId","race_Id")\
    .withColumnRenamed("constructorId","constructor_Id")\
    .withColumnRenamed("driverId","driver_Id")

#display(df_sprint_results)  
df_driver_standings=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/driver_standings.csv")\
    .withColumnRenamed("driverStandingsId","driverStandings_Id")\
    .withColumnRenamed("raceId","race_Id")\
    .withColumnRenamed("driverId","driver_Id")\
        
#display(df_driver_standings) 
df_drivers=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/drivers.csv")\
                   .withColumnRenamed("driverRef","driver_Ref")\
                   .withColumnRenamed("driverId","driver_Id")\
                   .withColumnRenamed("dob","DateOfBirth")\
                   .withColumn("FullName",concat(col("forename"),lit('_'),col("surname")))\
                   .drop("url")
#display(df_drivers) 
df_lap_times=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/lap_times.csv")\
                                  .withColumnRenamed("raceId","race_Id")\
                                  .withColumnRenamed("driverId","driver_Id")\
                                  .withColumnRenamed("time","Time")
#display(df_lap_times)                          
df_pit_stops=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/pit_stops.csv")\
                     .withColumnRenamed("raceId","race_Id")\
                     .withColumnRenamed("driverId","driver_Id")\
                     .withColumnRenamed("time","Time")
#display(df_pit_stops)
df_qualifying=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/qualifying.csv")\
                    .withColumnRenamed("qualifyId","qualify_Id")\
                    .withColumnRenamed("raceId","race_Id")\
                    .withColumnRenamed("driverId","driver_Id")\
                    .withColumnRenamed("constructorId","constructor_Id")
# display(df_qualifying)
df_races=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/races.csv")\
                .withColumnRenamed("circuitId","circuit_Id")\
                .withColumnRenamed("raceId","race_Id")\
                .withColumnRenamed("name","Name")\
                .withColumnRenamed("date","DateOfBirth")\
                .drop("url")

# display(df_races)
df_results=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/results.csv")\
                    .withColumnRenamed("resultId","result_Id")\
                    .withColumnRenamed("race_Id","race_Id")\
                    .withColumnRenamed("driverId","driverId")\
                    .withColumnRenamed("constructorId","constructor_Id")
        
# display(results)
df_seasons=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/seasons.csv")\
    .drop("url")
# display(df_seasons)
df_sprints=spark.read.format("csv").option("header",True).option("inferschema",True)\
                .load("dbfs:/tmp/source_db/sprint_results.csv")\
                    .withColumnRenamed("resultId","result_Id")\
                    .withColumnRenamed("race_Id","race_Id")\
                    .withColumnRenamed("driverId","driverId")\
                    .withColumnRenamed("constructorId","constructor_Id")
# display(df_sprints)
df_status=spark.read.format("csv").option("header",True).option("inferschema",True).load("dbfs:/tmp/source-db/status.csv")
# display(df_status)
