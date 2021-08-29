#!/bin/ksh
#########################################################################################################################################################
## SHELL      : DDL_DL_wac_canastas.sh
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

source="gnx"

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\timing
\echo creando tablas externas DataRaw y DataLake
DROP TABLE IF EXISTS ${SpectrumDatabaseRaw}.wac_canastas_flatfile;
DROP TABLE IF EXISTS ${SpectrumDatabaseDataLake}.wac_canastas;
EOF

psql "${TNS_CONNECT_REDSHIFT}" <<EOF
\set ON_ERROR_STOP on
\timing
\echo creando tablas externas DataRaw y DataLake

CREATE EXTERNAL TABLE ${SpectrumDatabaseRaw}.wac_canastas_flatfile(
NegocioId int,
TiendaId varchar(50),
EAN double precision,
SKU VARCHAR(15),
CodigoTipoCanasta VARCHAR(3),
FechaCanasta VARCHAR(10),
CodigoDescuento double precision)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 's3://${bucketDataRaw}.${source}/wac_canastas';
    \p \echo \echo 

CREATE EXTERNAL TABLE ${SpectrumDatabaseDataLake}.wac_canastas(
NegocioId int,
TiendaId varchar(50),
EAN double precision,
SKU VARCHAR(15),
CodigoTipoCanasta VARCHAR(3),
FechaCanasta varchar(10),
CodigoDescuento double precision)
STORED AS PARQUET
LOCATION 's3://${bucketDataLake}/wac_canastas';
    \p \echo \echo 
    
EOF
