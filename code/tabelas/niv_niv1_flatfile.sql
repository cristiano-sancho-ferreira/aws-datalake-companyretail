CREATE EXTERNAL TABLE csancho_datalake_raw_dev.niv_niv1_flatfile (
operacion_cd VARCHAR(3),
secuencial VARCHAR(10),
merc_nivel1_cd VARCHAR(16),
fecha_novedad DATE,
merc_nivel1_desc VARCHAR(100),
merc_nivel0_cd VARCHAR(16))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://csancho-datalake-raw-dev/niv_niv1'
