CREATE EXTERNAL TABLE csancho_datalake_raw_dev.niv_niv4_flatfile (
operacion_cd VARCHAR(1),
secuencial VARCHAR(10),
merc_nivel4_cd VARCHAR(16),
fecha_novedad DATE,
merc_nivel4_desc VARCHAR(100),
merc_nivel3_cd VARCHAR(16))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://csancho-datalake-raw-dev/niv_niv4'
