from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Define the schema for data from source -> sf_fire_calls.csv
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

if __name__ == "__main__":
    # create a SparkSession
    spark = (SparkSession
             .builder
             .appName("Read_CSV_Example")
             .getOrCreate())

    # create a DataFrame using the schema defined above, reading in the csv file
    sf_fire_file = "data/sf-fire-calls.csv"
    sf_fc_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
    # sf_fc_df.show(5, truncate=False)

    # Looking at calls that were not medical incidents, so fires might be the assumption
    not_fires_df = (sf_fc_df.select("IncidentNumber", "AvailableDtTm", 'CallType')
                    .where(col('CallType') != "Medical Incident"))
    not_fires_df.show(5, truncate=False)

    # How many distinct call types were recorded as causes of calls to fire department?
    (sf_fc_df.select("CallType").where(col("CallType").isNotNull())
     .agg(countDistinct("CallType").alias("DistinctCallTypes"))
     .show())

    (sf_fc_df
     .select("CallType")
     .where(col("CallType").isNotNull())
     .distinct()
     .show(10, False))

    # Make new df and look at response times with renamed columns
    call_rt_df = sf_fc_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
    (call_rt_df.select("ResponseDelayedinMins")
     .where(col("ResponseDelayedinMins") > 5)
     .show(5, False))

    # Convert the string data types in the file to SQL times and dates
    fc_ts_df = (call_rt_df.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                .drop("CallDate")
                .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
                .drop("WatchDate")
                .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                .drop("AvailableDtTm")
                )
    # See first 5 rows of 3 converted columns in new DF
    (fc_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False))

    # Find how many years of calls are in the data
    (fc_ts_df
     .select(year('IncidentDate'))
     .distinct()
     .orderBy(year('IncidentDate'))
     .show())

    # write transformed dataframe as a new parquet file
    # parquet_path = "data/parquets/txfrm_sf_fire_calls.parquet"
    # sf_fc_df.write.format("parquet").save(parquet_path)

    # alternative - write transformed data to a new SQL table (in a DB? or is this in spark memory/disk?)
    # parquet_table = <name of table>
    # sf_fc_df.write.format("parquet").saveAsTable(parquet_table)
