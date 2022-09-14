import pyspark.sql.functions as psf
from pyspark.sql.window import Window
from pyspark.sql.functions import col


def dailyLoad(spark, input_files, output_files):
    """
    This function is for fetching records from vehicles status and find the latest event type for a particular vehicle
    for that day and write them in partitioned vehicle_daily_load table:
    Arguments:
        spark : Spark Session
        input_files : Path to ingest Vehicle_events files
        output_files : Summarized files will be stored at this path
    return :
        write processed data into vehicle_daily_summarized
    """
    # Reading vehicles_status table and creating df. Later on filters can be applied on basis of date to filter only
    # that particular day data
    read_df = spark.read.table(f"{input_files}")

    # Creating new fields which can be used later for finding the latest event type for a particular vehicle
    modified_df = read_df.withColumn('day', psf.to_date('event_time')) \
        .withColumn('new_time', psf.date_format('event_time', 'HH:mm:ss')) \
        .withColumnRenamed('event_time', 'last_event_time') \
        .withColumnRenamed('event_type', 'last_event_type')

    # Used windowing function for fetching latest event type for a particular vehicle for given day
    windowSpec = Window.partitionBy("vehicle_id", "day").orderBy(col("new_time").desc())
    vehicle_df = modified_df.withColumn("rank", psf.rank().over(windowSpec)).filter("rank==1") \
        .select("vehicle_id", "day", "last_event_time", "last_event_type")

    vehicle_df.write.format("parquet").mode("overwrite").partitionBy("day").saveAsTable(f"{output_files}")

    print("Records loaded in vehicle_daily_summarized")
    vehicle_df.show(50, False)

    return
