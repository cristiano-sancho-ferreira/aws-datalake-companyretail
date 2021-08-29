CREATE EXTERNAL TABLE csancho_datalake_analytics_dev.niv_niv1 (
operacion_cd VARCHAR(3),
secuencial VARCHAR(10),
merc_nivel1_cd VARCHAR(16),
fecha_novedad VARCHAR(10),
merc_nivel1_desc VARCHAR(100),
merc_nivel0_cd VARCHAR(16))
STORED AS PARQUET
LOCATION 's3://csancho-datalake-analytics-dev/niv_niv1'
