#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_movin_motin.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.movin_motin_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.movin_motin;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.movin_motin_flatfile(
clase_movimiento varchar(3),
motivo_movimiento_desc varchar(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/movin_motin';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.movin_motin(
clase_movimiento varchar(3),
motivo_movimiento_desc varchar(20))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/movin_motin' 
    \p \echo \echo 
    
EOF
