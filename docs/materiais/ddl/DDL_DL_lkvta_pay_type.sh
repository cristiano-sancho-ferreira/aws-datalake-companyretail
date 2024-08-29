#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_lkvta_pay_type.sh
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

source="jan"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.lkvta_pay_type_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.lkvta_pay_type;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.lkvta_pay_type_flatfile(
payment_type_cd varchar(50),
payment_type_class_cd varchar(50),
payment_type_desc varchar(255),
payment_type_dist_ind varchar(2))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/lkvta_pay_type';
    \p \echo \echo   


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.lkvta_pay_type(
payment_type_cd varchar(50),
payment_type_class_cd varchar(50),
payment_type_desc varchar(255),
payment_type_dist_ind varchar(2))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/lkvta_pay_type';
    \p \echo \echo 
    
EOF
