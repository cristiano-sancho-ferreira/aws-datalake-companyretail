#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pricing_compe.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pricing_compe_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pricing_compe;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pricing_compe_flatfile(
codigo_competidor double precision,
nombre_competidor VARCHAR(255),
nombre_abreviado_competidor VARCHAR(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/pricing_compe';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pricing_compe(
codigo_competidor double precision,
nombre_competidor VARCHAR(255),
nombre_abreviado_competidor VARCHAR(50))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pricing_compe' 
    \p \echo \echo 
    
EOF
