CREATE EXTERNAL TABLE csancho_datalake_raw_dev.niv_niv0_flatfile (
operacion_cd VARCHAR(1),
secuencial VARCHAR(10),
merc_nivel0_cd VARCHAR(16),
fecha_novedad DATE,
merc_nivel0_desc VARCHAR(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://csancho-datalake-raw-dev/niv_niv0'
