import dlt
import reverse_geocoder as rg
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *

@pandas_udf(StringType())
def get_county_batch(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    coords = [(float(lat), float(lon)) 
              for lat, lon in zip(lat_series, lon_series) 
              if pd.notna(lat) and pd.notna(lon)]
    
    if not coords:
        return pd.Series([None] * len(lat_series))
    
    results = rg.search(coords)
    counties = [r.get('admin2', '').upper().replace(' COUNTY', '').strip() 
                for r in results]
    
    return pd.Series(counties)

# Stage 1: Stream and clean the data
@dlt.view(
    name="silver_earthquake_cleaned",
    comment="Cleaned earthquake data (streaming)"
)
def create_silver_earthquake_cleaned():
    earthquake_df = spark.readStream.table("earth_data.lakehouse.silver_earthquake")
    
    return earthquake_df

# Stage 2: Batch enrichment with county (Pandas UDF works better in batch)
@dlt.table(
    name="silver_with_location",
    comment="Earthquake data with county (batch enriched)"
)
def create_silver_earthquake_location():
    # Read as BATCH for better UDF performance
    earthquake_selected = dlt.read_stream("silver_earthquake_cleaned")

    earthquake_selected = (
        earthquake_selected
        .withColumn('time', (col('time') / 1000).cast(TimestampType()))
        .withColumn('updated', (col('updated') / 1000).cast(TimestampType()))
        .withColumn('latitude', col('latitude').cast('double'))
        .withColumn('longitude', col('longitude').cast('double'))
    )
    
    earthquake_with_location = (
        earthquake_selected
        .repartition(200)  # Improve parallelism
        .withColumn('county', get_county_batch(col('latitude'), col('longitude')))
    )
    
    return earthquake_with_location