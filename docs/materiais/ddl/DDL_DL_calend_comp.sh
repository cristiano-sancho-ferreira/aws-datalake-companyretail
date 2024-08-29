#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_calend_comp.sh
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

source="user"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.calend_comp_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.calend_comp;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.calend_comp_flatfile(
Calendar_Dt varchar(10),
Comparable_Calendar_Dt varchar(10),
Comparable_Calendar_Cd int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/calend_comp';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.calend_comp(
Calendar_Dt varchar(10),
Comparable_Calendar_Dt varchar(10),
Comparable_Calendar_Cd int)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/calend_comp'
    \p \echo \echo 
    
EOF
