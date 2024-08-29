#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_lkvta_bin_group.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.lkvta_bin_group_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.lkvta_bin_group;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.lkvta_bin_group_flatfile(
Promo_offer_credit_card_type_cd varchar(8), 
BIN_Cd varchar(20))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LOCATION 's3://${bucketDataRaw}.${source}/lkvta_bin_group';


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.lkvta_bin_group(
Promo_offer_credit_card_type_cd varchar(8), 
BIN_Cd varchar(20))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/lkvta_bin_group' 
    \p \echo \echo 
    
EOF
