#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_pricing_compeaso.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.pricing_compeaso_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.pricing_compeaso;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.pricing_compeaso_flatfile(
codigo_competidor double precision,
centro_cd VARCHAR(10),
articulo_cd VARCHAR(20),
codigo_barras_cd VARCHAR(18),
codigo_barras_cd_asoc VARCHAR(18),
desc_larga_arti VARCHAR(255))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/pricing_compeaso';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.pricing_compeaso(
codigo_competidor double precision,
centro_cd VARCHAR(10),
articulo_cd VARCHAR(20),
codigo_barras_cd VARCHAR(18),
codigo_barras_cd_asoc VARCHAR(18),
desc_larga_arti VARCHAR(255))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/pricing_compeaso' 
    \p \echo \echo 
    
EOF
