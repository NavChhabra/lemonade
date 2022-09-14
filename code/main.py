from pyspark.sql import SparkSession
import argparse
from env import get_env


def eventLoader_wrapper(spark, env):
    """
    Wrapper for ingesting daily vehicle events and status from JSON file
    Arguments:
        spark: Spark Session
        env: Environment variable which holds configured table names and path
    return:
        pass
    """
    from eventsloader import load_vehicles_event, load_vehicles_status

    # Env variables
    batch_home = env['BATCH_HOME']
    events_file = batch_home + '/input/' + env['EVENTS_FILENAME']
    status_file = batch_home + '/input/' + env['STATUS_FILENAME']
    events_checkpoint = env['EVENTS_CHECKPOINT']
    status_checkpoint = env['STATUS_CHECKPOINT']
    events_table = env['EVENTS_TABLE']
    status_table = env['STATUS_TABLE']

    vehicle_events = load_vehicles_event(spark, events_file, events_checkpoint, events_table)
    vehicle_status = load_vehicles_status(spark, status_file, status_checkpoint, status_table)

    vehicle_events.awaitTermination()
    vehicle_status.awaitTermination()

    return


def dailySummarized_wrapper(spark, env):
    """
    Wrapper for loading daily summarized data into vehicle_daily_summarized
    Arguments:
        spark: Spark Session
        env: Environment variable which holds configured table names and path
    return:
        pass
    """
    from dailysummarized import dailyLoad

    # Env variables
    input_table = env['EVENTS_TABLE']
    output_table = env['DAILY_LOAD_TABLE']

    dailyLoad(spark, input_table, output_table)

    return


def main():
    """
    Main function is for parsing arguments, instantiating spark session and invoking wrappers.
    """

    env = get_env()

    batch_home = env['BATCH_HOME']

    # Spark created and supplied with some additional conf to set some additional attributes like for dynamic partition
    # which will be required while loading data in vehicle_daily_summarized table
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

    parser = argparse.ArgumentParser(description="Main program to ingest events and load daily summarized table")
    subparser = parser.add_subparsers(help="sub-command help")

    parser_vehicle = subparser.add_parser("eventsloader", help="For ingesting daily vehicle events and status")
    parser_vehicle.set_defaults(func=eventLoader_wrapper)

    parser_dailyload = subparser.add_parser("dailysummarized", help="Module for loading summarized data")
    parser_dailyload.set_defaults(func=dailySummarized_wrapper)

    # Parsing arguments
    args = parser.parse_args()

    args.func(spark, env)

    return


if __name__ == "__main__":
    main()
