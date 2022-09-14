# Lemonade - Events ingestion

In this project I've used Spark Structured streaming to ingest continously produced events which are written in JSON file under inbound folder. Spark Structured streaming is a fault tolerant API for ingesting evnets from multiple sources, process them as per requirement in micro batches and load them into various kind of sources. Spark Structured streaming is best suited when we are working with the ingestion of JSON files and loading them in the Hive tables. It has capabilites to extract schema from JSON and create dataframe on top of it. It gives the libarty to add config for scalling up the app.

## Project structure

  - **Checkpoint** : This directory stores data for keeping the track of ingested files. When we run spark streaming jobs it keeps all the metadata related to which files have been ingested so far and which still needs to be processed. Spark continously check metadata after a specific interval of time and start ingestion as soon as new file arrives.
  - **Code** : It holds all the files which required for triggering spark streaming process.
      - **submit.sh** : It's the main entry point. This use spark-submit helps in passing additional arguments for creating a spark serssion. These additional config attributes like driver memory, executore memory, cores etc help in tunning the job. Value passed through them can bbe increased or decreased as per the load. If we are getting large number of files with huge number of events which will require larger set of resources to process them in that case we can increase number of executor, cores & their memory as per the requirement.
      - **main.py** : It's a python scripts in which wrappers for calling both modules __eventsloader__ & __dailysummarized__ is implemented.
      - **env.py** : Holds a dictionary of key & values. These are being used at various stages of the project.
       - **log4j.properties** : Store properties related to logging
      - **eventsloader.py** : It implements various functions which are used for creating two streams for ingesting continously produced JSON files. It basically use 3 step approach. First it creates a readStream for reading data from the input directory which actually produce a df. Second some transformation logic is applied on this df to explode and generate other fields. Third it write that transformed data in respective tables.
      - **dailysummarized.py** : It holds the pyspark code which reads data from ___vehicle_events___ tables. Apply windowing function for extracting vehicle with latest event type and latest event time for that particular date and load it in ___vehicle_daily_summarized___ table.
      - **spark-warehouse** : ___Due to unavailability of hadoop setup in my local machine___ using internal __spark-warehouse__ which spark uses to keep data for a particular table and creates managed table.
  - **input** : For this project using this as a landing folder where we will receive JSON files for __vehicle_events__ & __vehicle_status__
  - **main.py** : Transformation script written in python which handles operation like reading JSON files from input directory, transforming data as per the requirement and load final data in provided output directory in JSON format
  - **log4j.properties** : Stores properties related to logging

## Quick Start

Below command will to understand what parameters needs to be passed :
```
sh submit --help
```

To start the streaming process for consuming JSON files use below command :
```
sh submit eventsload
```
It will start streaming processes to consume JSON files for __vehicle_events__ & __vehicle_status__ which are available or will be loaded under __input__ folder. This will be a continous streaming process which can be scheduled to ingest data at a specific interval of times.

In order to run daily summarized job use below command:
```
sh submit dailysummarized
```
This job extract data from __vehicle-events__ table, fetch latest event record for every vehicle and load it into partitioned __vehicle_daily_summarized__ table. This job can be scheduled to run once at the end of the day to populate table with latest records.

