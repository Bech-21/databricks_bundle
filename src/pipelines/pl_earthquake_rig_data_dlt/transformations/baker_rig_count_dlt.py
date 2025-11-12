import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name ='bronze_rig_count',
    table_properties = {'quality': 'bronze'},
    comment ='Raw rig count data ingested from the source system'
)
def create_bronze_rig_count():
    return (
        spark.readStream
            .format('cloudFiles')
            .option('cloudFiles.format','csv')
            .option('cloudFiles.inferColumnTypes','true')
            .option('header','true')  # Add header option for CSV
            .load('/Volumes/earth_data/bronze/operationaldata/rig_count/')
            .withColumnRenamed('Rig Count Value', 'rig_count_value')
            .select(
               '*',
                col('_metadata.file_path').alias('input_file_path'),
                current_timestamp().alias('ingest_timestamp')
            )
    )

@dlt.table(
    name ='silver_rig_clean',
    comment ='Cleaned rig data',
    table_properties = {'quality': 'silver'}
)
@dlt.expect_or_drop('valid_County','county IS NOT NULL')
def create_silver_rig_clean():
    return (
        dlt.read_stream('bronze_rig_count')
            .select(
                col('Country').alias('country'),
                col('County').alias('county'),
                col('Basin').alias('basin'),
                col('DrillFor').alias('drill_for'),
                col('State/Province').alias('state_or_province'),
                col('Location').alias('location'),
                col('Trajectory').alias('trajectory'),
                col('US_PublishDate').cast('date').alias('date'),
                col('rig_count_value').cast('int').alias('rig_count'),
                col('ingest_timestamp').alias('created_date')  # Use ingest_timestamp as created_date
            )
    )



# Create SCD Type 2 table
dlt.create_streaming_table(
    name ='silver_rig_count',
    comment ='SCD Type 2 rig_count',
    table_properties = {'quality': 'silver'}
)

# Apply CDC with SCD Type 2
dlt.apply_changes(
    target ='silver_rig_count',
    source ='silver_rig_clean',
    keys = ['country','county','basin','drill_for','state_or_province','location','trajectory'],
    sequence_by ='date',
    stored_as_scd_type = 2
)