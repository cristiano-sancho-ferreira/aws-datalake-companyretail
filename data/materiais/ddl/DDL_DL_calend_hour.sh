#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_calend_hour.sh
## Autor      : Jackson.Silva(ST IT Consulting)
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

source="user"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.calend_hour_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.calend_hour;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.calend_hour_flatfile(
hour_id VARCHAR(8),
hour_desc VARCHAR(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/calend_hour';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.calend_hour(
hour_id VARCHAR(8),
hour_desc VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/calend_hour' 
    \p \echo \echo 
    
EOF
