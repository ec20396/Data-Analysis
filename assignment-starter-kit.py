import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from datetime import datetime
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import date_format
from pyspark.sql.functions import to_date, count, col, when, mean, month, lit
from graphframes import *



def fieldcleansing(dataframe):

  if (dataframe.first()['taxi_type']=='yellow_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    
    
  elif (dataframe.first()['taxi_type']=='green_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
  
  
  dataframe = dataframe.filter((dataframe["trip_distance"] >= 0) & (dataframe["fare_amount"] >= 0))
  return dataframe 
  
    

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Do It
    yellow_tripdata_path = f"s3a://data-repository-bkt/ECS765/nyc_taxi/yellow_tripdata/2023"
    yellow_tripdata_df = spark.read.csv(yellow_tripdata_path, header=True, inferSchema=True)

    green_tripdata_path = f"s3a://data-repository-bkt/ECS765/nyc_taxi/green_tripdata/2023"
    green_tripdata_df = spark.read.csv(green_tripdata_path, header=True, inferSchema=True)

    taxi_zone_lookup_path = f"s3a://data-repository-bkt/ECS765/nyc_taxi/taxi_zone_lookup.csv"
    taxi_zone_lookup = spark.read.csv(taxi_zone_lookup_path, header=True, inferSchema=True)





    # checking and removing any null values or wrong format in the dataset and cleaning them for further processing
    yellow_tripdata_df = fieldcleansing(yellow_tripdata_df)
    green_tripdata_df  =  fieldcleansing (green_tripdata_df)

    
    
    # start working the the task as per instruction


    
    #Task 1
    
    #Join yellow trip data with taxi zone lookup information to add pickup and dropoff details
    yellow_taxi_zone_df = yellow_tripdata_df.join(taxi_zone_lookup, yellow_tripdata_df.PULocationID == taxi_zone_lookup.LocationID, "left") \
        .withColumnRenamed("Borough", "Pickup_Borough") \
        .withColumnRenamed("Zone", "Pickup_Zone") \
        .withColumnRenamed("service_zone", "Pickup_service_zone") \
        .drop("LocationID") \
        .join(taxi_zone_lookup, yellow_tripdata_df.DOLocationID == taxi_zone_lookup.LocationID, "left") \
        .withColumnRenamed("Borough", "Dropoff_Borough") \
        .withColumnRenamed("Zone", "Dropoff_Zone") \
        .withColumnRenamed("service_zone", "Dropoff_service_zone") \
        .drop("LocationID")

    #Join Green trip data with taxi zone lookup information to add pickup and dropoff details
    green_taxi_zone_df = green_tripdata_df.join(taxi_zone_lookup, green_tripdata_df.PULocationID == taxi_zone_lookup.LocationID, "left") \
        .withColumnRenamed("Borough", "Pickup_Borough") \
        .withColumnRenamed("Zone", "Pickup_Zone") \
        .withColumnRenamed("service_zone", "Pickup_service_zone") \
        .drop("LocationID") \
        .join(taxi_zone_lookup, green_tripdata_df.DOLocationID == taxi_zone_lookup.LocationID, "left") \
        .withColumnRenamed("Borough", "Dropoff_Borough") \
        .withColumnRenamed("Zone", "Dropoff_Zone") \
        .withColumnRenamed("service_zone", "Dropoff_service_zone") \
        .drop("LocationID")
    
    # Print schema and row/column counts
    print("Yellow Taxi Data Schema:")
    yellow_taxi_zone_df.printSchema()
    print("Number of rows:", yellow_taxi_zone_df.count())
    print("Number of columns:", len(yellow_taxi_zone_df.columns))

    print("Green Taxi Data Schema:")
    green_taxi_zone_df.printSchema()
    print("Number of rows:", green_taxi_zone_df.count())
    print("Number of columns:", len(green_taxi_zone_df.columns))







    
    #Task 2 Here: Relies on Task 1
    
    #Illustrate the pickup locations in each NYC borough for yellow taxis as CSV DataFrame. 
    yellow_pickup_counts = yellow_taxi_zone_df.groupBy("Pickup_Borough").agg(count("*").alias("Yellow_Pickup_Count"))
    yellow_pickup_counts.show()
    now = datetime.now() 
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S") 
    
    output_path_yellow_pickup = 's3a://{}/task2/yellow_pickup_counts_{}'.format(s3_bucket, date_time)
    yellow_pickup_counts.coalesce(1).write.csv(output_path_yellow_pickup, header=True, mode='overwrite')

    #Display the number of pickup locations in each NYC borough for green taxis as CSV DataFrame.
    green_pickup_counts = green_taxi_zone_df.groupBy("Pickup_Borough").agg(count("*").alias("Green_Pickup_Count"))
    green_pickup_counts.show()
    output_path_green_pickup = 's3a://{}/task2/green_pickup_counts_{}'.format(s3_bucket, date_time)
    green_pickup_counts.coalesce(1).write.csv(output_path_green_pickup, header=True, mode='overwrite')
    
    #Depict the dropoff locations in each NYC borough for yellow taxis as CSV DataFrame.
    yellow_dropoff_counts = yellow_taxi_zone_df.groupBy("Dropoff_Borough").agg(count("*").alias("Yellow_Dropoff_Count"))
    yellow_dropoff_counts.show()
    output_path_yellow_dropoff = 's3a://{}/task2/yellow_dropoff_counts_{}'.format(s3_bucket, date_time)
    yellow_dropoff_counts.coalesce(1).write.csv(output_path_yellow_dropoff, header=True, mode='overwrite')
    
    # Show the number of dropoff locations in each NYC borough for green taxis as CSV DataFrame 
    green_dropoff_counts = green_taxi_zone_df.groupBy("Dropoff_Borough").agg(count("*").alias("Green_Dropoff_Count"))
    green_dropoff_counts.show()
    output_path_green_dropoff = 's3a://{}/task2/green_dropoff_counts_{}'.format(s3_bucket, date_time)
    green_dropoff_counts.coalesce(1).write.csv(output_path_green_dropoff, header=True, mode='overwrite')




    

    #Task 3
    def filter_yellow_trips(name, pickup_datetime, fare_amount, trip_distance):
        try:
            fare = float(fare_amount)
            distance = float(trip_distance)
            pickup_date = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S").date()
    
            # Filter for trips within the specified time window and fare/distance conditions
            return datetime(2023, 1, 1).date() <= pickup_date <= datetime(2023, 1, 7).date() and fare > 50 and distance < 1
        except ValueError:
            return False
    
    filter_udf = udf(filter_yellow_trips, returnType=BooleanType())# Create a UDF using the udf function for filtering
    filtered_yellow_trips_df = yellow_tripdata_df.filter(filter_udf("passenger_count", "tpep_pickup_datetime", "fare_amount", "trip_distance"))   # Apply the UDF for filtering records
    daily_trip_counts = filtered_yellow_trips_df.groupBy(date_format("tpep_pickup_datetime", "yyyy-MM-dd").alias("Date")).count().orderBy("Date")# Count the trips on each date
    daily_trip_counts.show()
    now = datetime.now() 
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    output_path_daily_trip_counts = 's3a://{}/task3/daily_trip_counts_{}'.format(s3_bucket, date_time)
    daily_trip_counts.coalesce(1).write.csv(output_path_daily_trip_counts, header=True, mode='overwrite')






    
    #Task 4
    def get_top_boroughs(df, column_name, top_5):
        return df.groupBy(column_name).count().orderBy("count", ascending=False).limit(top_5)# Group the DataFrame by the specified column, count occurrences, order by count in descending order, and limit top_5

    # Get the top boroughs for yellow taxi pickups, display the result, and save to my bucket
    top_yellow_pickup = get_top_boroughs(yellow_taxi_zone_df, "Pickup_Borough", 5)
    top_yellow_pickup.show()
    now = datetime.now() 
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    output_path_top_yellow_pickup = 's3a://{}/task4/top_yellow_pickup_{}'.format(s3_bucket, date_time)
    top_yellow_pickup.coalesce(1).write.csv(output_path_top_yellow_pickup, header=True, mode='overwrite')

    # Get the top boroughs for yellow taxi drop-offs, display the result, and save to my bucket
    top_yellow_dropoff = get_top_boroughs(yellow_taxi_zone_df, "Dropoff_Borough", 5)
    top_yellow_dropoff.show()
    output_path_top_yellow_dropoff = 's3a://{}/task4/top_yellow_dropoff_{}'.format(s3_bucket, date_time)
    top_yellow_dropoff.coalesce(1).write.csv(output_path_top_yellow_dropoff, header=True, mode='overwrite')

    # Get the top boroughs for green taxi pickups, display the result, and save to my bucket
    top_green_pickup = get_top_boroughs(green_taxi_zone_df, "Pickup_Borough", 5)
    top_green_pickup.show()
    output_path_top_green_pickup = 's3a://{}/task4/top_green_pickup_{}'.format(s3_bucket, date_time)
    top_green_pickup.coalesce(1).write.csv(output_path_top_green_pickup, header=True, mode='overwrite')

    # Get the top boroughs for green taxi drop-offs, display the result, and save to my bucket
    top_green_dropoff = get_top_boroughs(green_taxi_zone_df, "Dropoff_Borough", 5)
    top_green_dropoff.show()
    output_path_top_green_dropoff = 's3a://{}/task4/top_green_dropoff_{}'.format(s3_bucket, date_time)
    top_green_dropoff.coalesce(1).write.csv(output_path_top_green_dropoff, header=True, mode='overwrite')




    
    #Task 5
    def extract_anomalies(dataframe):
        # Define the date range for June 2023
        start_date = "2023-06-01"
        end_date = "2023-07-01"
        high_anomaly_threshold=1.1
        low_anomaly_threshold=0.9
        
        dataframe = dataframe.filter(
            (col("tpep_pickup_datetime") >= start_date) & (col("tpep_pickup_datetime") < end_date)) # Extract day from timestamp
        
        dataframe = dataframe.withColumn("pickup_day", to_date("tpep_pickup_datetime"))# Filter data for the specified date range in June 2023
        daily_trip_counts = dataframe.groupBy("pickup_day").agg(count("*").alias("total_trips"))# Group by day and calculate total trips
        mean_trips = daily_trip_counts.agg({"total_trips": "mean"}).collect()[0]["avg(total_trips)"]# Calculate mean trips
        print(mean_trips)
        anomalies = daily_trip_counts.withColumn(
                "anomaly",
                when(col("total_trips") > mean_trips * high_anomaly_threshold, "High Anomaly")
                .when(col("total_trips") < mean_trips * low_anomaly_threshold, "Low Anomaly")
                .otherwise("Normal")
            )  # Identify anomalies
        
        anomalies_only = anomalies.filter((col("anomaly") == "High Anomaly") | (col("anomaly") == "Low Anomaly")) # Filter out only the anomalies
        anomalies_only = anomalies_only.orderBy("pickup_day") # Order by the date

        return anomalies_only.select("pickup_day", "total_trips")


    anomalies_df = extract_anomalies(yellow_tripdata_df) # Uses function on yellow taxi dataframe to extract anomalies

    anomalies_df.show()
    now = datetime.now() 
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    output_path_anomalies = 's3a://{}/task5/anomalies_df_{}'.format(s3_bucket, date_time)
    anomalies_df.coalesce(1).write.csv(output_path_anomalies, header=True, mode='overwrite')





    
    #Task 6
    def fare_per_mile(yellow_df):
        start_date = "2023-03-01"
        end_date = "2023-04-01"
        yellow_df = yellow_df.filter(
            (col("tpep_pickup_datetime") >= start_date) & (col("tpep_pickup_datetime") < end_date))  # Filter data for March 2023
        yellow_df = yellow_df.orderBy(col("tpep_pickup_datetime"))
    
        # Calculate fare per mile
        fare_per_mile_df = yellow_df.withColumn("fare_per_mile", col("fare_amount") / col("trip_distance"))
    
        # Calculate mean fare per mile and add as a new column
        mean_fare_per_mile = fare_per_mile_df.agg(mean(col("fare_per_mile")).alias("mean_fare_per_mile")).collect()[0]["mean_fare_per_mile"]
    
        # Add a column to identify outliers
        outlier_threshold = 1000 * mean_fare_per_mile
        fare_per_mile_df = fare_per_mile_df.withColumn("outlier", col("fare_per_mile") > outlier_threshold)

        fare_per_mile_df = fare_per_mile_df.select("fare_per_mile","outlier")
    
        return fare_per_mile_df



    fare_df = fare_per_mile(yellow_tripdata_df) # Uses function on yellow taxi dataframe to extract anomalies
    fare_df.show()
    now = datetime.now() 
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    output_path_fare_df = 's3a://{}/task6/fare_df_{}'.format(s3_bucket, date_time)
    fare_df.coalesce(1).write.csv(output_path_fare_df, header=True, mode='overwrite')



    

    #Task 7
    def solo_passenger(taxi_df):
        total_trips = taxi_df.count()# Calculate total trip count for the dataset
        solo_trips = taxi_df.filter(col("passenger_count") == 1).count()# Calculate the number of solo trips
        percentage_solo = (solo_trips / total_trips) * 100 #Calculate percentage of solo passengers
        return percentage_solo
    
    percentage_solo_yellow = solo_passenger(yellow_tripdata_df) #Saves percentage of solo passengers for yellow taxi in variable
    percentage_solo_green = solo_passenger(green_tripdata_df) #Saves percentage of solo passengers for greentaxi in variable
    print(f"Percentage of solo trips for yellow Taxi's: {percentage_solo_yellow:.2f}%") # Prints percentage of solo passengers of yellow taxi 
    print(f"Percentage of solo trips for Green Taxi's: {percentage_solo_green:.2f}%") # Prints percentage of solo passengers of green taxi



    
    #Task 8
    def yellow_most_trips_month(yellow_taxi_df):
        yellow_taxi_df = yellow_taxi_df.withColumn("pickup_month", date_format(col("tpep_pickup_datetime"), "MMMM"))# Add a new column for the pickup month
        trips_per_month = yellow_taxi_df.groupBy("pickup_month").count().orderBy("pickup_month")# Group the data by pickup month and count the trips for each month
        most_trips_row = trips_per_month.orderBy(col("count").desc()).first() # Find the month with the most trips
        most_trips_month = most_trips_row["pickup_month"]
    
        most_trips_df = trips_per_month.filter(col("pickup_month") == most_trips_month) \
                                       .select("pickup_month", col("count").alias("trip_count")) # Create a new DataFrame with data for the month with the most trips
    
        return most_trips_df
    
    def green_most_trips_month(green_taxi_df):
        green_taxi_df = green_taxi_df.withColumn("pickup_month", date_format(col("lpep_pickup_datetime"), "MMMM"))# Add a new column for the pickup month
        trips_per_month = green_taxi_df.groupBy("pickup_month").count().orderBy("pickup_month")# Group the data by pickup month and count the trips for each month
        most_trips_row = trips_per_month.orderBy(col("count").desc()).first()
        most_trips_month = most_trips_row["pickup_month"]# Find the month with the most trips
    
        most_trips_df = trips_per_month.filter(col("pickup_month") == most_trips_month) \
                                       .select("pickup_month", col("count").alias("trip_count"))# Create a new DataFrame with data for the month with the most trips
        return most_trips_df

    




    month_yellowtaxi_df = yellow_most_trips_month(yellow_tripdata_df) # Uses function to extract month with most trips for yellow taxi
    month_greentaxi_df = green_most_trips_month(green_tripdata_df)# Uses function to extract month with most trips for green taxi
    now = datetime.now() 
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    output_path_month_yellowtaxi_df = 's3a://{}/task10/month_yellowtaxi_df_{}'.format(s3_bucket, date_time)
    month_yellowtaxi_df.coalesce(1).write.csv(output_path_month_yellowtaxi_df,header=True, mode='overwrite')
   
    output_path_month_greentaxi_df = 's3a://{}/task10/month_greentaxi_df_{}'.format(s3_bucket, date_time)
    month_greentaxi_df.coalesce(1).write.csv(output_path_month_greentaxi_df, header=True, mode='overwrite')


    

    # Stop the Spark session
    spark.stop()











    
