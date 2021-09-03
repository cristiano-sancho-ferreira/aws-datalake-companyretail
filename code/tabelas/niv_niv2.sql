CREATE EXTERNAL TABLE companyretail_datalake_dev_analytics.niv_niv2 (
operacion_cd VARCHAR(1),
secuencial VARCHAR(10),
merc_nivel2_cd VARCHAR(16),
fecha_novedad VARCHAR(10),
merc_nivel2_desc VARCHAR(100),
merc_nivel1_cd VARCHAR(16))
STORED AS PARQUET
LOCATION 's3://companyretail-datalake-dev-lake/niv_niv0'
