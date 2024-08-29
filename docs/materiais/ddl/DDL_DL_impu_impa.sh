#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_impu_impa.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.impu_impa_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.impu_impa;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.impu_impa_flatfile (
operacion_cd VARCHAR(1),
tipo_impuesto_cd VARCHAR(10),
tipo_alicuota_cd VARCHAR(10),
tipo_alicuota_desc VARCHAR(100),
alicouta_pct double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/impu_impa';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.impu_impa (
operacion_cd VARCHAR(1),
tipo_impuesto_cd VARCHAR(10),
tipo_alicuota_cd VARCHAR(10),
tipo_alicuota_desc VARCHAR(100),
alicouta_pct double precision)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/impu_impa';
    \p \echo \echo 
EOF
