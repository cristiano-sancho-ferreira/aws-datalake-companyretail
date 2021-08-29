#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_healthy.sh
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

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.healthy_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.healthy;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.healthy_flatfile(
  item_healthy_type_id varchar(50), 
  item_healthy_type_desc varchar(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/healthy';
  
 CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.healthy(
  item_healthy_type_id varchar(50), 
  item_healthy_type_desc varchar(100))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/healthy';
    \p \echo \echo 
    
EOF
