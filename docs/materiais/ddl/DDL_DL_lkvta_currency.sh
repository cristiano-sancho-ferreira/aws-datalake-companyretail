#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_lkvta_currency.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.lkvta_currency_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.lkvta_currency;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.lkvta_currency_flatfile (
Currency_Cd VARCHAR(10),
Currency_Name VARCHAR(100),
Currency_Desc VARCHAR(200),
Currency_ISO_Cd VARCHAR(10),
Currency_Alternate_Cd VARCHAR(10))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/lkvta_currency';
    \p \echo \echo 

	
CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.lkvta_currency(
Currency_Cd VARCHAR(10),
Currency_Name VARCHAR(100),
Currency_Desc VARCHAR(200),
Currency_ISO_Cd VARCHAR(10),
Currency_Alternate_Cd VARCHAR(10))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/lkvta_currency';
    \p \echo \echo 
    
EOF
