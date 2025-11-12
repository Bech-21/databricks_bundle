
CREATE OR REFRESH STREAMING TABLE bronze_earthquake
COMMENT 'Raw customers data ingested from the source API as json'
TBLPROPERTIES ('quality' = 'bronze')
AS 
SELECT *,
       _metadata.file_path AS input_file_path,
       CURRENT_TIMESTAMP AS ingestion_timestamp
FROM cloud_files(
  '/Volumes/earth_data/bronze/operationaldata/',
  'json',
  map('cloudFiles.inferColumnTypes', 'true',
      'multiLine', 'true'
  )
);



CREATE OR REFRESH STREAMING TABLE silver_earthquick_clean(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_properties EXPECT (properties IS NOT NULL)

)
COMMENT "Cleaned earthquake data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT id,
       geometry,
       properties,
       input_file_path,
       CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp
  FROM STREAM(earth_data.lakehouse.bronze_earthquake);


CREATE STREAMING TABLE silver_earthquake
AS
SELECT id,
      geometry.coordinates[0] AS longitude,
      geometry.coordinates[1] AS latitude,
      geometry.coordinates[2] AS elevation,
      properties.title AS title,
      properties.place AS place_description,
      properties.sig AS sig,
      properties.mag AS magnitude,
      properties.magType AS magType,
      properties.time AS time,
      properties.updated AS updated,
      properties.sources AS sources
   FROM STREAM(silver_earthquick_clean);

