import pyspark.sql.functions as psf


def readData(spark, file_name):
    """
    Take spark session and file name as an argument which needs to create a spark read stream. Then it creates a read
    stream & infer schema from the file. It takes some other options like maxFilePerTrigger to tell how many should be
    processed at once.
    Arguments:
        spark : Spark Session
        file_name : file which needs to be look for and ingested to fetch the events
    return :
        write processed data into vehicle_daily_load
    """
    print("********* FILE NAME : ", file_name)
    read_df = spark \
        .readStream \
        .option("inferSchema", "true") \
        .option("maxFilesPerTrigger", 1) \
        .option("multiline", "true") \
        .json(f"{file_name}" + "_*")

    return read_df


def writeData(input_df, checkpoint_path, table_name):
    """
    It creates write stream to continuously write data in the respective table as soon as new file arrives with the
    new events. This takes
    Arguments:
        input_df : Dataframe which needs to be written
        checkpoint_path : A path where spark streaming keeps metadata regarding which files have been processed so far
        and written to the respective table.
        table_name : In which events need to be written
    return :
        write processed data into vehicle_daily_load
    """
    write_df = input_df \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}") \
        .toTable(f"{table_name}")

    return write_df


def load_vehicles_event(spark, file_name, checkpoint_path, table_name):
    """
    This function fetches respective vehicle events files from the respective folder in continuous manner as soon as
    they arrive and load them in respective table after processing them. It takes file_name which needs to be fetch
    and checkpoint path where spark keeps the metadata of processed files.
    Arguments:
        spark : Dataframe which needs to be written
        file_name : file which needs to be look for and ingested to fetch the events'
        checkpoint_path : A path where spark streaming keeps metadata regarding which files have been processed so far
        and written to the respective table.
        table_name : vehicle_events table which holds all the ingested vehicle events
    return :
        write processed data into vehicle_daily_load
    """
    vehicle_df = readData(spark, file_name)

    vehicle_df = vehicle_df.select(psf.explode('vehicles_events').alias('tmp')).select(
        'tmp.*')

    vehicle_df.printSchema()

    vehicle_df = writeData(vehicle_df, checkpoint_path, table_name)

    return vehicle_df


def load_vehicles_status(spark, file_name, checkpoint_path, table_name):
    """
    This function fetches respective vehicle status files from the respective folder in continuous manner as soon as
    they arrive and load them in respective table after processing them. It takes file_name which needs to be fetch
    and checkpoint path where spark keeps the metadata of processed files.
    Arguments:
        spark : Dataframe which needs to be written
        file_name : file which needs to be look for and ingested to fetch the events'
        checkpoint_path : A path where spark streaming keeps metadata regarding which files have been processed so far
        and written to the respective table.
        table_name : vehicle_status table which holds all the ingested vehicle status
    return :
        write processed data into vehicle_daily_load
    """
    vehicle_df = readData(spark, file_name)

    vehicle_df = vehicle_df.select(psf.explode('vehicle_status').alias('tmp')).select('tmp.*')

    vehicle_df.printSchema()

    vehicle_df = writeData(vehicle_df, checkpoint_path, table_name)

    return vehicle_df
