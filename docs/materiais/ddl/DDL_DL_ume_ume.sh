#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_ume_ume.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.ume_ume_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.ume_ume;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.ume_ume_flatfile (
operacion_cd VARCHAR(1),
umedida_cd VARCHAR(10),
dimension_cd VARCHAR(10),
umedida_iso_cd VARCHAR(10),
umedida_desc VARCHAR(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/ume_ume';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.ume_ume (
operacion_cd VARCHAR(1),
umedida_cd VARCHAR(10),
dimension_cd VARCHAR(10),
umedida_iso_cd VARCHAR(10),
umedida_desc VARCHAR(100))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/ume_ume';
    \p \echo \echo 
    
EOF
