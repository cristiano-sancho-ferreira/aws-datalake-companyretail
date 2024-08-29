#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_wac_detaccom.sh
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
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.wac_detaccom_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.wac_detaccom;
EOF


psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.wac_detaccom_flatfile (
CodigoDescuento double precision,
TiendaId varchar(50),
EAN double precision,
SKU VARCHAR(15),
NumeroPagina int,
PrevistoVentaBruta double precision,
PrevistoVentaNeta double precision,
PrevistoUnidades double precision,
PrevistoMargenPesos double precision,
NegocioId VARCHAR(3))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/wac_detaccom';
    \p \echo \echo 


CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.wac_detaccom (
CodigoDescuento double precision,
TiendaId varchar(50),
EAN double precision,
SKU VARCHAR(15),
NumeroPagina int,
PrevistoVentaBruta double precision,
PrevistoVentaNeta double precision,
PrevistoUnidades double precision,
PrevistoMargenPesos double precision,
NegocioId VARCHAR(3))
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/wac_detaccom';
    \p \echo \echo 
    
EOF
