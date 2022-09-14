from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import IntegerType, DateType, StringType, StructType, MapType
import pyspark.sql.functions as psf
from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc
from env import get_env


def readData(spark, file_name):
    read_df = spark \
        .readStream \
        .option("inferSchema", "true") \
        .option("maxFilesPerTrigger", 1) \
        .option("multiline", "true") \
        .json(f"{file_name}" + "_*")

    return read_df


def writeData(input_df, checkpoint_path, table_name):
    write_df = input_df \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}") \
        .toTable(f"{table_name}")

    #         .option("path", "/Users/navneetsingh/Downloads/lemonade/output/event/") \
    #         .format("console") \

    return write_df


def load_vehicles_event(spark, file_name, checkpoint_path):
    vehicle_df = readData(spark, file_name)

    vehicle_df = vehicle_df.select(psf.explode('vehicles_events').alias('tmp')).select(
        'tmp.*')  # .withColumn('event_extra_data', )

    vehicle_df.printSchema()

    vehicle_df = writeData(vehicle_df, checkpoint_path, table_name='vehicle_event')

    return


def load_vehicles_status(spark, file_name, checkpoint_path):
    vehicle_df = readData(spark, file_name)

    vehicle_df = vehicle_df.select(psf.explode('vehicle_status').alias('tmp')).select('tmp.*')

    vehicle_df.printSchema()

    vehicle_df = writeData(vehicle_df, checkpoint_path, table_name='vehicle_status')

    return

    '''
    return vehicle_df \
        .writeStream \
        .format("JSON") \
        .option("path", "/Users/navneetsingh/Downloads/lemonade/output/status/") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_path}") \
        .trigger(processingTime='7 seconds') \
        .start()
    '''


if __name__ == "__main__":
    env = get_env()

    batch_home = env['BATCH_HOME']
    events_file = batch_home + env['EVENTS_FILENAME']
    status_file = batch_home + env['STATUS_FILENAME']
    events_checkpoint = env['EVENTS_CHECKPOINT']
    status_checkpoint = env['STATUS_CHECKPOINT']

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("lemonade") \
        .config("hive.sql.warehouse.dir",
                f"{batch_home}" + "/code/spark-warehouse/") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # spark.sql("set spark.sql.streaming.schemaInference=true")

    load_vehicles_event(spark, events_file, events_checkpoint)

    load_vehicles_status(spark, status_file, status_checkpoint)

    spark.streams.awaitAnyTermination()
