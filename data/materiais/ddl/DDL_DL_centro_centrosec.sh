#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_centro_centrosec.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.centro_centrosec_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.centro_centrosec;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.centro_centrosec_flatfile (
Tienda VARCHAR(10),
Seccion VARCHAR(10),
Formato VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/centro_centrosec';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.centro_centrosec (
Tienda VARCHAR(10),
Seccion VARCHAR(10),
Formato VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/centro_centrosec';
    \p \echo \echo 
    
EOF
