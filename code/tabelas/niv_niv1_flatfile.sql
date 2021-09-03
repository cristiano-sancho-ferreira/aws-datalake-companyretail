CREATE EXTERNAL TABLE companyretail_datalake_dev_raw.niv_niv1_flatfile (
operacion_cd VARCHAR(3),
secuencial VARCHAR(10),
merc_nivel1_cd VARCHAR(16),
fecha_novedad DATE,
merc_nivel1_desc VARCHAR(100),
merc_nivel0_cd VARCHAR(16))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://companyretail-datalake-dev-raw/niv_niv1'
