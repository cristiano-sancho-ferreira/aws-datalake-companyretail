CREATE EXTERNAL TABLE csancho_datalake_analytics_dev.niv_niv0 (
operacion_cd VARCHAR(1),
secuencial VARCHAR(10),
merc_nivel0_cd VARCHAR(16),
fecha_novedad VARCHAR(10),
merc_nivel0_desc VARCHAR(100))
STORED AS PARQUET
LOCATION 's3://csancho-datalake-analytics-dev/niv_niv0'
