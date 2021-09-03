CREATE EXTERNAL TABLE companyretail_datalake_dev_raw.niv_niv3_flatfile (
operacion_cd VARCHAR(1),
secuencial VARCHAR(10),
merc_nivel3_cd VARCHAR(16),
fecha_novedad DATE,
merc_nivel3_desc VARCHAR(100),
merc_nivel2_cd VARCHAR(16))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://companyretail-datalake-dev-raw/niv_niv3'
