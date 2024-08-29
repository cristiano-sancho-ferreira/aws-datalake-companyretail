#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_arti_ean.sh
## Autor      : Silva
## Finalidad  : Crear las tablas externas de Data Raw y Data Lake
## 		
## ParÃ¡metros : No Hay
## Retorno    : 0 - OK
##              9 - NOK - Error de ejecuciÃ³n
## Historia   : Fecha     | Descripcion
##              ----------|-----------------------------------------------------------------
##              10/07/2018| Código inicial
###########################################################################################
#set -x
## Carga las variables de entorno
. ${HOME}/ETL/.parameters

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.arti_ean_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.arti_ean;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.arti_ean_flatfile (
articulo_cd VARCHAR(18),
umedida_cd VARCHAR(3),
secuencial VARCHAR(5),
codigo_barra_cd VARCHAR(18),
ean_principal VARCHAR(1),
codigo_barra_venta VARCHAR(18))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/arti_ean';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.arti_ean (
articulo_cd VARCHAR(18),
umedida_cd VARCHAR(3),
secuencial VARCHAR(5),
codigo_barra_cd VARCHAR(18),
ean_principal VARCHAR(1),
codigo_barra_venta VARCHAR(18))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/arti_ean';
    \p \echo \echo 
    
EOF
